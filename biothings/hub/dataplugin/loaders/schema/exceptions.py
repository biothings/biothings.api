"""
Custom exceptions for the different types of issues that users can
run into when crafting a manifest file. We want to be detailed here
as this is fairly forward/user facing so the more helpful the error message
the less debugging we might have to do later with a newer user creating a dataplugin
"""

import jsonschema


class ManifestTypeException(Exception):
    """
    Exception for mismatched type in the manifest
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        python_json_type_mapping = {
            dict: "object",
            list: "array",
            tuple: "array",
            int: "integer",
            str: "string",
            float: "float",
            bool: "boolean",
        }
        current_instance_type = type(validation_error.instance)
        current_instance_type = python_json_type_mapping.get(current_instance_type, str(current_instance_type))
        type_error_message = (
            "Discovered a manifest typing error that prevents loading. "
            f"Please update the {list(validation_error.absolute_path)} section of the manifest. "
            f"EXPECTED {validation_error.validator} [{validation_error.validator_value}] | "
            f"DISCOVERED {validation_error.validator} [{current_instance_type}]"
        )
        super().__init__(type_error_message)


class ManifestMissingPropertyException(Exception):
    """
    Exception for missing a required property in the manifest
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        breakpoint()
        pass


class ManifestAdditionalPropertyException(Exception):
    """
    Exception for additional property not specified in the schema
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        breakpoint()
        pass


class ManifestMinimumRequiredItemsException(Exception):
    """
    Exception for array properties that aren't populated the minimum required
    length
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        breakpoint()
        pass


def determine_validation_error_category(validation_error: jsonschema.exceptions.ValidationError):
    """
    Examines the validation exception properties to determine what type of error occured

    We then return a more specific Exception with details on how to correct the manifest
    error
    """
    validation_exception_mapping = {
        "type": ManifestTypeException,
        "required": ManifestMissingPropertyException,
        "additionalProperties": ManifestAdditionalPropertyException,
        "minItems": ManifestMinimumRequiredItemsException,
    }
    manifest_exception = validation_exception_mapping.get(validation_error.validator, None)
    exception = manifest_exception(validation_error)
    return exception
