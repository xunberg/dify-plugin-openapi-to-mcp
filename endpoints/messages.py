import json
import requests
from fastmcp.utilities import openapi
from typing import Any, Dict, List, Mapping
from werkzeug import Request, Response
from dify_plugin import Endpoint

# 配置日志记录
import logging
from logging.handlers import RotatingFileHandler
import os
import time

# 配置日志记录
# 计算 10M 的字节数
max_size = 10 * 1024 * 1024  # 10MB
# 计算 7 天的秒数
backup_count = 7 * 24 * 60 * 60  # 7 days

# 自定义日志处理器，在 RotatingFileHandler 基础上增加时间检查
class TimeRotatingFileHandler(RotatingFileHandler):
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=False):
        super().__init__(filename, mode, maxBytes, backupCount, encoding, delay)
        self.backup_count = backupCount

    def doRollover(self):
        super().doRollover()
        # 清理过期的日志文件
        self._remove_old_logs()

    def _remove_old_logs(self):
        dir_name, base_name = os.path.split(self.baseFilename)
        file_list = os.listdir(dir_name)
        now = time.time()
        for f in file_list:
            if f.startswith(base_name):
                file_path = os.path.join(dir_name, f)
                if os.path.isfile(file_path):
                    file_age = now - os.path.getmtime(file_path)
                    if file_age > self.backup_count:
                        os.remove(file_path)

# 创建文件处理器
file_handler = TimeRotatingFileHandler(
    "mcp_messages.log",
    maxBytes=max_size,
    backupCount=backup_count,
    encoding="utf-8"
)

# 创建流处理器
stream_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        file_handler,
        stream_handler
    ]
)


class MessageEndpoint(Endpoint):

    def _invoke(self, r: Request, values: Mapping, settings: Mapping) -> Response:
        """
        Invokes the endpoint with the given request.

        Args:
            r (Request): The incoming request object.
            values (Mapping[str, Any]): Additional values.
            settings (Mapping[str, Any]): Configuration settings.

        Returns:
            Response: The response object.
        """
        try:
            logging.info("starting _invoke method")
            # 获取 OpenAPI 模式
            openAPI_schema = self._get_openAPI_schema(settings)
            openapi_service_base, mcp_tools = self._get_service_and_tools(
                openAPI_schema
            )

            session_id = r.args.get("session_id")
            if session_id is None:
                logging.warning("session_id is missing in the request")
            data = r.get_json()  # 使用 get_json 方法，避免异常时崩溃
            logging.info(f"Received request: {data}")
            if data is None:
                logging.error("Failed to parse JSON data from the request")
                return Response(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "error": {"code": -32700, "message": "Parse error"},
                        }
                    ),
                    status=400,
                    content_type="application/json",
                )

            # 处理不同 method 请求
            # if data.get("method") == "notifications/initialized":
            #     return Response("", status=202, content_type="application/json")
            response = self._handle_method_request(
                data, openapi_service_base, mcp_tools
            )
            if session_id:
                self.session.storage.set(session_id, json.dumps(response).encode())
            return Response("", status=202, content_type="application/json")
        except Exception as e:
            logging.error(f"Error in _invoke: {str(e)}", exc_info=True)
            return Response(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "error": {"code": -32603, "message": "Internal error"},
                    }
                ),
                status=500,
                content_type="application/json",
            )

    def _get_openAPI_schema(self, settings: Mapping[str, Any]) -> Dict[str, Any]:
        """
        获取 OpenAPI 模式。

        Args:
            settings (Mapping[str, Any]): Configuration settings.

        Returns:
            Dict[str, Any]: The OpenAPI schema.
        """
        openAPI_schema = None
        try:
            schema_input = settings.get("openapi_schema")
            if schema_input is None:
                logging.error("Invalid in _get_openAPI_schema")
                raise ValueError("Invalid openapi_schema")
            if schema_input.startswith("http") == False:
                openAPI_schema = json.loads(schema_input)
            else:
                openAPI_scheme_url = schema_input
                if not openAPI_scheme_url or not openAPI_scheme_url.startswith("http"):
                    logging.error(
                        "Invalid input parameters: URL is null or not start with http"
                    )
                    raise ValueError(
                        "Invalid input parameters: URL is null or not start with http"
                    )
                # 读取 openAPI_scheme_url 的内容
                response = requests.get(openAPI_scheme_url)
                response.raise_for_status()
                openAPI_schema = response.json()
        except json.JSONDecodeError:
            logging.error("Invalid JSON input in _get_openAPI_schema")
            raise ValueError("Invalid JSON input")
        except requests.RequestException as e:
            logging.error(f"Request failed in _get_openAPI_schema: {str(e)}")
            raise ValueError(f"Request failed: {str(e)}")
        if openAPI_schema is None:
            logging.error("Invalid input parameters: OpenAPI schema is None")
            raise ValueError("Invalid input parameters: OpenAPI schema is None")
        return openAPI_schema

    def _get_service_and_tools(self, openAPI_schema: Dict[str, Any]):
        """
        获取服务基础信息和工具列表。

        Args:
            openAPI_schema (Dict[str, Any]): The OpenAPI schema.

        Returns:
            Tuple[Dict[str, Any], List[Dict[str, Any]]]: The service base and tools list.
        """
        services = openAPI_schema.get("servers", [])
        if not services:
            logging.error("No servers found in OpenAPI schema")
            raise ValueError("No servers found in OpenAPI schema")
        openapi_service_base = services[0]
        mcp_tools = get_mcp_tools_from_openAPI(openAPI_schema)
        return openapi_service_base, mcp_tools

    def _handle_method_request(
        self,
        data: Dict[str, Any],
        openapi_service_base: Dict[str, Any],
        mcp_tools: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        处理不同 method 请求。

        Args:
            data (Dict[str, Any]): The request data.
            openapi_service_base (Dict[str, Any]): The service base.
            mcp_tools (List[Dict[str, Any]]): The list of tools.

        Returns:
            Dict[str, Any]: The response data.
        """
        method = data.get("method")
        logging.info(f"Handling method: {method}")
        if method == "initialize":
            return self._handle_initialize(data)
        elif method == "notifications/initialized":
            return {"jsonrpc": "2.0", "id": data.get("id"), "result": {}}
        elif method == "tools/list":
            return self._handle_tools_list(data, mcp_tools)
        elif method == "tools/call":
            return self._handle_tools_call(data, openapi_service_base, mcp_tools)
        else:
            logging.warning(f"Unsupported method: {method}")
            return {
                "jsonrpc": "2.0",
                "id": data.get("id"),
                "error": {"code": -32001, "message": "unsupported method"},
            }

    def _handle_initialize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理 initialize 请求。

        Args:
            data (Dict[str, Any]): The request data.

        Returns:
            Dict[str, Any]: The response data.
        """
        return {
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "experimental": {},
                    "prompts": {"listChanged": False},
                    "resources": {"subscribe": False, "listChanged": False},
                    "tools": {"listChanged": False},
                },
                "serverInfo": {"name": "Dify", "version": "1.3.0"},
            },
        }

    def _handle_tools_list(
        self, data: Dict[str, Any], mcp_tools: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        处理 tools/list 请求。

        Args:
            data (Dict[str, Any]): The request data.
            mcp_tools (List[Dict[str, Any]]): The list of tools.

        Returns:
            Dict[str, Any]: The response data.
        """
        logging.info(f"Handling tools/list mcp_tools: {mcp_tools}")
        return {
            "jsonrpc": "2.0",
            "id": data.get("id"),
            "result": {"tools": mcp_tools},  # 修正结果格式
        }

    def _handle_tools_call(
        self,
        data: Dict[str, Any],
        openapi_service_base: Dict[str, Any],
        mcp_tools: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        处理 tools/call 请求。

        Args:
            data (Dict[str, Any]): The request data.
            openapi_service_base (Dict[str, Any]): The service base.
            mcp_tools (List[Dict[str, Any]]): The list of tools.

        Returns:
            Dict[str, Any]: The response data.
        """
        tool_name = data.get("params", {}).get("name")
        arguments = data.get("params", {}).get("arguments", {})
        try:
            # 调用工具
            call_result = make_openAPI_call(
                openapi_service_base,
                mcp_tools,
                tool_name,
                arguments,
            )
            final_result = {"type": "text", "text": str(call_result)}
            return {
                "jsonrpc": "2.0",
                "id": data.get("id"),
                "result": {"content": [final_result], "isError": False},
            }
        except Exception as e:
            logging.error(f"Error in _handle_tools_call: {str(e)}")
            return {
                "jsonrpc": "2.0",
                "id": data.get("id"),
                "error": {"code": -32000, "message": str(e)},
            }


def make_openAPI_call(
    openapi_service_base: Dict[str, Any],
    openapi_tool_list: List[Dict[str, Any]],
    tool_name: str,
    arguments: Dict[str, Any],
) -> str:
    """
    Calls an OpenAPI tool with the given arguments.
    Args:
        openapi_service_base: The base URL of the OpenAPI service.
        openapi_tool_list: The list of tools available in the OpenAPI service.
        tool_name: The name of the tool to call.
        arguments: The arguments to pass to the tool.
    Returns:
        The response from the tool as a JSON string.
    """
    # 参数校验
    if not openapi_service_base or not openapi_tool_list or not tool_name:
        raise ValueError(
            "Invalid input parameters: openapi_service_base, openapi_tool_list, or tool_name is empty"
        )

    try:
        # 查找工具
        openAPI_tool = next(
            (tool for tool in openapi_tool_list if tool["name"] == tool_name), None
        )
        if openAPI_tool is None:
            raise ValueError("Invalid tool name")

        # 构建 URL 和请求参数
        url, request_parameters = build_url_and_params(
            openapi_service_base, openAPI_tool, arguments
        )
        logging.info(
            f"api tool_name:{tool_name} call: url:{url} params: {request_parameters}"
        )

        # 发送请求
        response = send_request(openAPI_tool["method"], url, request_parameters)

        # 直接获取响应的原始文本内容
        result = response.text
        logging.info(f"api call result: {result}")
        return result
    except Exception as e:
        logging.error(f"Error calling tool: {str(e)}")
        raise ValueError(f"Error calling tool: {str(e)}")


def build_url_and_params(
    openapi_service_base: Dict[str, Any],
    openAPI_tool: Dict[str, Any],
    arguments: Dict[str, Any],
) -> (str, Dict[str, Any]):
    """
    Build the URL and request parameters for the API call.
    """
    tool_api_path = openAPI_tool.get("path")
    tool_method_type = openAPI_tool.get("method")
    tools_input_parameters = openAPI_tool.get("inputSchema", {}).get("properties", {})
    request_parameters = {}

    for param_name, param_schema in tools_input_parameters.items():
        param_location = param_schema.get("param_location")
        if param_location == "path":
            tool_api_path = tool_api_path.replace(
                f"{{{param_name}}}", arguments.get(param_name, "")
            )
        else:
            request_parameters[param_name] = arguments.get(param_name, "")

    url = f"{openapi_service_base.get('url')}{tool_api_path}"
    return url, request_parameters


def send_request(
    method: str, url: str, request_parameters: Dict[str, Any]
) -> requests.Response:
    """
    Send an HTTP request based on the given method.
    """
    try:
        if method.lower() == "get":
            response = requests.get(url, params=request_parameters)
        elif method.lower() == "post":
            response = requests.post(url, json=request_parameters)
        else:
            response = requests.request(method.upper(), url, json=request_parameters)

        # 检查响应状态码
        response.raise_for_status()  # 若状态码不是 2xx，会抛出异常
        return response
    except requests.RequestException as e:
        raise ValueError(f"Request failed: {str(e)}")


def get_mcp_tools_from_openAPI(openAPI_schema: dict) -> list:
    """
    Extracts the tools from the OpenAPI schema.
    """
    http_routes = openapi.parse_openapi_to_http_routes(openAPI_schema)
    tools = []
    for http_route in http_routes:
        tool = build_tool_from_http_route(http_route)
        tools.append(tool)
    return tools


def build_tool_from_http_route(http_route: openapi.HTTPRoute) -> dict:
    """
    Extracts the tools from the OpenAPI schema.
    """
    operation_id = http_route.operation_id
    description = http_route.description or http_route.summary
    input_schema = combine_schemas(http_route)

    tool = {
        "name": operation_id,
        "description": description,
        "inputSchema": input_schema,
        "method": http_route.method,
        "path": http_route.path,
    }

    return tool


def combine_schemas(route: openapi.HTTPRoute) -> dict[str, Any]:
    """
    Combines parameter and request body schemas into a single schema.

    Args:
        route: HTTPRoute object

    Returns:
        Combined schema dictionary
    """
    properties = {}
    required = []

    # Add path parameters
    for param in route.parameters:
        if param.required:
            required.append(param.name)
        param.schema_["param_location"] = param.location
        properties[param.name] = param.schema_

    # Add request body if it exists
    if route.request_body and route.request_body.content_schema:
        # For now, just use the first content type's schema
        content_type = next(iter(route.request_body.content_schema))
        body_schema = route.request_body.content_schema[content_type]
        body_props = body_schema.get("properties", {})
        for prop_name, prop_schema in body_props.items():
            prop_schema["param_location"] = "body"
            properties[prop_name] = prop_schema
        if route.request_body.required:
            required.extend(body_schema.get("required", []))

    return {
        "type": "object",
        "properties": properties,
        "required": required,
    }
