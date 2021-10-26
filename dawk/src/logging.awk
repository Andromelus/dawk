@namespace "dawk_logging"

function info(message) {
    print "INFO - " message >> "dawk.log"
}

function error(message) {
    print "ERROR - " message >> "dawk.log"
}

function warning(message) {
    print "WARNING - " message >> "dawk.log"
}