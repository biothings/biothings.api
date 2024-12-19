import importlib
import os
from typing import Any, Dict, Iterable

from pydantic import ValidationError

try:
    import black

    black_avail = True
except ImportError:
    black_avail = False


def generate_date_validator(date_fields: str) -> str:
    fields_str = ", ".join(f'"{field}"' for field in date_fields)
    return f"""
    @field_validator({fields_str})
    @classmethod
    def date_validator(cls, v):
        try:
            if isinstance(v, list):
                return [parse(date) for date in v]
            else:
                return parse(v)
        except Exception as e:
            raise ValueError(f"Invalid date format: {{v}}") from e
"""


def generate_base_model(model_name: str) -> str:
    return f"""class {model_name}(BaseModel):
"""


def generate_key_name(k: str, v: str) -> str:
    return f"""    {k}: Optional[Union[{v}, List[{v}]]] = None
"""


def generate_model(schema: Dict[str, Any], model_name: str) -> str:
    es_to_pydantic = {
        "text": "str",
        "keyword": "str",
        "long": "int",
        "integer": "int",
        "short": "int",
        "byte": "int",
        "double": "float",
        "float": "float",
        "half_float": "float",
        "scaled_float": "float",
        "date": "str",
        "boolean": "bool",
        "binary": "bytes",
        "integer_range": "tuple",
        "float_range": "tuple",
        "long_range": "tuple",
        "double_range": "tuple",
        "date_range": "tuple",
        "ip_range": "tuple",
    }
    date_fields = []
    base_model = generate_base_model(model_name)
    for k, v in schema.items():
        if isinstance(v, dict) and "properties" in v.keys():
            base_model += generate_key_name(k, k.capitalize())
        else:
            base_model += generate_key_name(k, es_to_pydantic.get(v["type"], Any))
            if v["type"] == "date":
                date_fields.append(k)
    if date_fields:
        base_model += generate_date_validator(date_fields)
    return base_model + "\n\n"


def create_pydantic_model(schema: Dict[str, Any], model_name: str):
    base_imports = """from typing import List, Optional, Union

from dateutil.parser import parse
from pydantic import BaseModel, field_validator

"""

    def parse_schema(schema: Dict[str, Any], model_name="", model="") -> Dict[str, Any]:
        for field_name, field_info in schema.items():
            if "properties" in field_info:
                model = parse_schema(field_info["properties"], field_name.capitalize(), model) + model
            else:
                model = generate_model(schema, model_name)
        return model

    model = parse_schema(schema)
    model = model + generate_model(schema, model_name)
    if black_avail:
        model = black.format_str(base_imports + model, mode=black.Mode())
    else:
        raise ImportError('"black" package is required for exporting formatted code.')
    return model


def commit_validator(model: str, validation_path: str, name: str):
    """Write the Pydantic model to a file
    Args:
        model: Pydantic model
        path: path to write the model
    """
    # create directory if it doesn't exist
    if not os.path.exists(validation_path):
        os.makedirs(validation_path)
    model_path = os.path.join(validation_path, f"{name}_model.py")
    with open(model_path, "w") as f:
        f.write(model)


def import_validator(model_path: str, klass: str):
    """Import the Pydantic model
    Args: validation_path: path to import the model
    klass: class name of the model
    """
    spec = importlib.util.spec_from_file_location("model_module", model_path)
    model_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(model_module)
    return getattr(model_module, klass)


def validate_documents(model, docs: Iterable):
    """Validate the documents using a validator model
    Args:
        model: Pydantic model
        docs: iterable of documents to validate
    """
    errors = []

    for doc in docs:
        try:
            model.model_validate(doc)
        except ValidationError as e:
            for error in e.errors():
                if "Input should be a valid list" not in error["msg"]:
                    errors.append(error)

    if errors:
        raise ValidationError.from_exception_data(doc["_id"], line_errors=errors)
