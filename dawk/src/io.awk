@namespace "dawk_io"

function makedir(path) {
    dawk_logging::info("creating directory " path)
    if (path == "") {
        dawk_logging::error("path empty in dawk_io makedir")
        exit 1
    }
    command = "mkdir -p " path
    while ((command | getline) > 0) {
        dawk_logging::error("an error occured during dir creation")
        print
    }
    close(command)
}

function removedirrec(path) {
    system("rm -r " path)
}

function removefile(path) {
    system("rm " path)
}

function copy_file(src, dest) {
    dawk_logging::info("copying file " src " to "  dest)
    cpcommand = "cp " src " " dest
    cpcommand | getline
    close(cpcommand)
}

function extract_file_portion(input_file, start, stop, output_file) {
    if (input_file == "") {
        dawk_logging::error("file extraction - intput file is empty")
        exit 1
    }
    if (output_file == "") {
        dawk_logging::error("file extraction - intput file is empty")
        exit 1
    }
    dawk_logging::info("reading file " input_file " from line " start " to line " stop " output to " output_file)
    
    read_count = 0
    while ((getline line < input_file) > 0) {
        if (read_count >= start && read_count <= stop) {
            print line >> output_file
        }
        read_count = read_count + 1
    }
    close(input_file)
}

function create_file(path) {
    system("touch " path)
}