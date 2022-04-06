HUB_DB_BACKEND = {
    "module": "biothings.utils.sqlite3",
    "sqlite_db_folder": "."
}

DATA_HUB_DB_DATABASE = ".hubdb"

# descONE
ONE = 1

#* section alpha *#
B = "B"

C = "C"  # ends with space should be stripped descC

# not a param, not uppercase
Two = 2

#* section beta *#
# descD_D
D_D = "D"

#* section gamma *#

# descE.
E = "E"

#* section beta *#

# descF.
# back to beta section.
F = "F"

#* *#
# reset section
G = "G"

# this is a secret param
#- invisible -#
INVISIBLE = "hollowman"

# hide the value, not the param
#- hide -#
PASSWORD = "1234"

# it's readonly
#- readonly -#
READ_ONLY = "written in stone"

# it's read-only and value is hidden, not the param
#- readonly -#
#- hide -#
READ_ONLY_PASSWORD = "can't read the stone"

# invisible has full power
# read-only is not necessary anyways
#- readonly
#- invisible -#
INVISIBLE_READ_ONLY = "evaporated"

# special param, by default config is read-only
# but we want to test modification
CONFIG_READONLY = False

LOG_FOLDER = "/tmp/biothings.api_tests/"
