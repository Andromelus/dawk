
# $ awk -f shuffle.awk -v MAX_WORKERS=3 -v BASE_PATH="./" -v SHUFFLE_KEY=1 -F "-" -v SHUFFLE_SEED=123456 ../user.awk
BEGIN {
    if (BASE_PATH == "") {
        print "base path not provided"
        exit 1
    }
    if (SHUFFLE_KEY == "") {
        print "shuffle key not provided"
        exit 1
    }
    if (MAX_WORKERS == "" || MAX_WORKERS == 0) {
        print "max workers not provided"
        exit 1
    }
    if (SHUFFLE_SEED == "") {
        print "shuffle seed not provided"
        exit 1
    }
    srand(SHUFFLE_SEED)
    for(i=0;i<128;i++) {
        STR_TO_ASCII[sprintf("%c",i)]=i
    }
}

function str_to_ascii(to_convert) {
    split(to_convert, chars, "")
    ascii = ""
    for (i=1;i<=length(to_convert); i++) {
        ascii = ascii sprintf("%03d", STR_TO_ASCII[chars[i]])
    }
    return ascii
}

{
    if (PARTITION[$1] == "") {
        ascii_key = str_to_ascii($1)
        PARTITION[$1] = ascii_key % MAX_WORKERS + 1
    }

    # print "key " $1 " p " PARTITION[$1] " file " FILENAME " " ascii_key>> "partititon_report"
    print $0 >> BASE_PATH"workers/" PARTITION[$1]"/shuffle_partition/data"
}