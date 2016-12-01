
SCHEMA = {
    "name": "gene",
    "type": "record",
    "fields": [
        {
            "name": "refseq",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "refseq_item",
                    "fields": [
                        {
                            "name": "genomic",
                            "type": [
                                "null",
                                "string",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "@esmappings": {
                                "include_in_all": "false",
                                "index": "no",
                                "type": "string"
                            }
                        },
                        {
                            "name": "rna",
                            "type": [
                                "null",
                                "string",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "@esmappings": {
                                "copy_to": ["refseq"],
                                "analyzer": "refseq_analyzer",
                                "type": "string"
                            }
                        },
                        {
                            "name": "protein",
                            "type": [
                                "null",
                                "string",
                                {
                                    "type": "array",
                                    "items": "string"
                                }
                            ],
                            "@esmappings": {
                                "copy_to": ["refseq"],
                                "analyzer": "refseq_analyzer",
                                "type": "string"
                            }
                        },
                        {
                            "name": "translation",
                            "type": [
                                "null",
                                {
                                    "name": "refseq_translation_item",
                                    "type": "record",
                                    "fields": [
                                        {
                                            "name": "rna",
                                            "type": ["null", "string", {"type":"array", "items":"string"}]
                                        }, 
                                        {
                                            "name": "protein",
                                            "type": ["null", "string", {"type":"array", "items":"string"}]
                                        }
                                    ]
                                },
                                {
                                    "type": "array",
                                    "items": "refseq_translation_item"
                                } 
                            ],
                            "@esmappings": {
                                "include_in_all": "false",
                                "type": "object",
                                "enabled": "false"                                
                            }
                        }
                    ]
                }
            ]
        },
        {
            "name": "humancyc",
            "type": [
                "null",
                {
                    "type": "map",
                    "values": "string"
                }
            ],
            "@esmappings": {}
        }],
    "@esmappings": {
        "dynamic": "false",
        "_timestamp": {
            "enabled": "true"
        }
    },
    "@jsonld": {},
    "@license": {},
    "@description": {},
    "@alias": {}
}
