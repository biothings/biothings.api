import importlib
import os
from typing import Any, Dict, Iterable, List, Union

from pydantic import ValidationError

try:
    import black

    black_avail = True
except ImportError:
    black_avail = False


def generate_date_validator(date_fields: str) -> str:
    fields_str = ", ".join(f'"{field}"' for field in date_fields)

    return f"""
    @field_validator({fields_str}, mode="before")
    @classmethod
    def date_validator(cls, v):
        if isinstance(v, list):
            if all(isinstance(item, str) for item in v):
                try:
                    return [parse(item) for item in v]
                except Exception as e:
                    raise ValueError(f"Invalid date format: {{v}}") from e
            elif all(isinstance(item, datetime) for item in v) or all(isinstance(item, date) for item in v):
                return v
            else:
                raise ValueError(
                    f"All items in the list must be of the same type (str, date, or datetime): {{v}}"
                )
        else:
            if isinstance(v, str):
                try:
                    return parse(v)
                except Exception as e:
                    raise ValueError(f"Invalid date format: {{v}}") from e
            elif isinstance(v, datetime) or isinstance(v, date):
                return v
            else:
                raise ValueError(
                    f"Invalid date: {{v}} of type: {{type(v)}} must be of type str, date, or datetime"
                )

"""


def generate_base_model(model_name: str) -> str:
    return f"""class {model_name}(BaseModel):
"""


def generate_key_name(k: str, v: Union[str, List[str]]) -> str:
    if isinstance(v, list):
        union_types = ", ".join(v)
        list_union_types = ", ".join(f"List[{t}]" for t in v)
        return f"""    {k}: Optional[Union[{union_types}, {list_union_types}]] = None
"""
    else:
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
        "date": ["str", "date", "datetime"],
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
            base_model += generate_key_name(k, model_name + "_" + k.capitalize())
        else:
            base_model += generate_key_name(k, es_to_pydantic.get(v["type"], "Any"))
            if v["type"] == "date":
                date_fields.append(k)
    if date_fields:
        base_model += generate_date_validator(date_fields)
    return base_model + "\n\n"


def create_pydantic_model(schema: Dict[str, Any], model_name: str):
    base_imports = """# This is an auto-generated file used to validate data for a specific data source.
from datetime import date, datetime
from typing import Any, List, Optional, Union

from dateutil.parser import parse
from pydantic import BaseModel, field_validator


"""

    def parse_schema(schema: Dict[str, Any], model_name="", model="") -> Dict[str, Any]:
        for field_name, field_info in schema.items():
            if "properties" in field_info:
                model = parse_schema(
                    field_info["properties"],
                    model_name + "_" + field_name.capitalize(),
                    model,
                )

        model = model + generate_model(schema, model_name)
        return model

    model = parse_schema(schema, model_name)
    if black_avail:
        model = black.format_str(base_imports + model, mode=black.Mode())
    else:
        raise ImportError('"black" package is required for exporting formatted code.')
    return model


def commit_validator(model: str, validation_path: str, src_name: str):
    """Write the Pydantic model to a file
    Args:
        model: Pydantic model
        path: path to write the model
    """
    # create directory if it doesn't exist
    if not os.path.exists(validation_path):
        os.makedirs(validation_path)
    model_path = os.path.join(validation_path, f"{src_name}_model.py")
    with open(model_path, "w") as f:
        f.write(model)


def import_validator(model_path: str, klass: str):
    """Import the Pydantic model
    Args: validation_path: path to import the model
    klass: class name of the model corresponding to the uploader source name
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
