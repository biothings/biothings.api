"""
Custom exceptions for the different types of issues that users can
run into when crafting a manifest file. We want to be detailed here
as this is fairly forward/user facing so the more helpful the error message
the less debugging we might have to do later with a newer user creating a dataplugin
"""

import jsonschema

from biothings.utils.loggers import get_logger

logger, _ = get_logger(__name__)


class ManifestTypeException(jsonschema.exceptions.ValidationError):
    """
    Exception for mismatched type in the manifest
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        logger.debug(validation_error)
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
        schema_error_message = (
            "Discovered a manifest typing error that prevents loading. "
            f"Please update the {list(validation_error.absolute_path)} section of the manifest. "
            f"Expected {validation_error.validator} <{validation_error.validator_value}> | "
            f"Discovered {validation_error.validator} <{current_instance_type}>"
        )
        super().__init__(schema_error_message)


class ManifestMissingPropertyException(jsonschema.exceptions.ValidationError):
    """
    Exception for missing a required property in the manifest
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        # ***NOTE***
        # Not including the debug message with the validation error as it's massive
        # including the entire schema, with the least confusing error message of
        # the custom error messages
        schema_error_message = (
            "Discovered a missing property that is required for the manifest. "
            "Please add the missing section to the manifest before proceeding. "
            f"[{validation_error.message}]\n"
            "The manifest schema can be referenced by running:\n\t"
            f"`biothings-cli manifest schema`"
        )
        super().__init__(schema_error_message)


class ManifestMutuallyExclusivePropertyException(jsonschema.exceptions.ValidationError):
    """
    Exception for dealing with the oneOf structure within the JSONSchema.
    Effectively requires for a property to be mutually exclusive of other properties
    One of many may exist, but not multiple of the many.

    Usage:
        - dumper.data_url typing (array or string)
        - oneOf(uploader | uploaders). At the root of the schema, you can either have an uploader
        object or an uploaders array. Not both since they are logically covering the same manifest
        content
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        # ***NOTE***
        # Not including the debug message with the validation error as it's massive
        # including the entire schema, with the a fairly straight forward error message
        exclusive_properties = {
            list(required_property.values())[0][0] for required_property in validation_error.validator_value
        }
        schema_error_message = (
            "Multiple exclusive properties were provided in the same manifest. "
            f"Please only include one of the following properties in the manifest: {exclusive_properties}"
        )
        super().__init__(schema_error_message)


class ManifestAdditionalPropertyException(jsonschema.exceptions.ValidationError):
    """
    Exception for additional property not specified in the schema
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        # ***NOTE***
        # Not including the debug message with the validation error as it's massive
        # including the entire schema, with the a fairly straight forward error message
        schema_error_message = (
            "Discovered unexpected additional property(ies) in the manifest. "
            "Please remove the additional property(ies) before proceeding. "
            f"{validation_error.message}"
        )
        super().__init__(schema_error_message)


class ManifestMinimumRequiredItemsException(jsonschema.exceptions.ValidationError):
    """
    Exception for array properties that aren't populated the minimum required
    length
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        logger.debug(validation_error)
        property_path = ".".join(list(validation_error.absolute_path))
        schema_error_message = (
            f"{property_path} required at minimum {validation_error.validator_value} "
            "object in the array and cannot be empty. "
            "Please populate the array in the manifest before proceeding"
        )
        super().__init__(schema_error_message)


class ManifestIncorrectEnumException(jsonschema.exceptions.ValidationError):
    """
    Exception for invalid enum value passed to property with explicilty enumerated
    list of values
    """

    def __init__(self, validation_error: jsonschema.exceptions.ValidationError):
        logger.debug(validation_error)
        schema_error_message = (
            f"Invalid value [{validation_error.instance}] for field <{validation_error.path[-1]}>. "
            f"Please use one of following values: {validation_error.validator_value}. "
            f"Full schema field path: {'.'.join(validation_error.absolute_schema_path)}. "
            "Please update the enum field in the manifest before proceeding"
        )
        super().__init__(schema_error_message)


def determine_validation_error_category(validation_error: jsonschema.exceptions.ValidationError):
    """
    Examines the validation exception properties to determine what type of error occured

    We then return a more specific Exception with details on how to correct the manifest
    error
    """
    validation_exception_mapping = {
        "type": ManifestTypeException,
        "required": ManifestMissingPropertyException,
        "oneOf": ManifestMutuallyExclusivePropertyException,
        "additionalProperties": ManifestAdditionalPropertyException,
        "minItems": ManifestMinimumRequiredItemsException,
        "enum": ManifestIncorrectEnumException,
    }
    manifest_exception = validation_exception_mapping.get(validation_error.validator, None)
    if manifest_exception is None:
        exception = validation_error
    else:
        exception = manifest_exception(validation_error)
    return exception
