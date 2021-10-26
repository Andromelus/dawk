@namespace "dawk_master"

function get_data_file_size(file_path) {
    command = "ls -l " file_path
    size = 0
    loop_index = 0
    while ((command |& getline) > 0) {
        size=$5
        loop_index = loop_index + 1
    }
    close(command)
    if (loop_index != 1) {
        dawk_logging::error("Multiple files found with this pattern")
    } else {
        dawk_logging::info("get_data_file_size ok")
        dawk_logging::info("loopindex " loop_index " size " size)
        return size
    }
}

function get_nbr_line_file(file_path) {
    command = "wc -l " file_path
    size = 0
    loop_index = 0
    while ((command |& getline) > 0) {
        size=$1
        loop_index = loop_index = 1
    }
    if (loop_index != 1) {
        dawk_logging::error("Multiple files found with this pattern")
    } else {
        dawk_logging::info("get_nbr_line_file ok")
        dawk_logging::info("loopindex " loop_index " size " size)
        return size
    }
}

function process_file(file_path, file_content_base_path){
    file_lines = get_nbr_line_file(file_path)
    medium_nbr_lines = file_lines / awk::NBR_WORKERS
    rounded = math::round(medium_nbr_lines)

    assigned_qtt_of_lines = 0
    # Assign quantity of lines to process to each worker
    for (worker_number = 1; worker_number <= awk::NBR_WORKERS; worker_number++) {
        # last worker. Assign what's left
        if (worker_number == awk::NBR_WORKERS) {
            nbr_lines_per_worker[worker_number] = file_lines - assigned_qtt_of_lines
        } else {
            nbr_lines_per_worker[worker_number] = rounded
            assigned_qtt_of_lines = assigned_qtt_of_lines + nbr_lines_per_worker[worker_number]
        }
    }

    for (worker in nbr_lines_per_worker) {
        dawk_logging::info("Line assignment: w:" worker " ---- " nbr_lines_per_worker[worker])
    }

    # Directory creation for the workers
    dawk_io::makedir("./workers")
    for (worker in nbr_lines_per_worker) {
        if (nbr_lines_per_worker[worker] != 0) {
            dawk_io::makedir("./workers/"worker)
            dawk_io::makedir("./workers/"worker"/shuffle_partition")
            dawk_io::create_file("./workers/"worker"/shuffle_partition/data")
        }
        
    }

    dawk_logging::info("splitting data between workers")

    # Extraction of input file sections for each worker
    start_line = 0
    for (worker in nbr_lines_per_worker) {
        if (nbr_lines_per_worker[worker] != 0) {
            output_file = "./workers/"worker"/to_process"
            stop_line = nbr_lines_per_worker[worker] == 1 ? start_line + nbr_lines_per_worker[worker] : start_line + nbr_lines_per_worker[worker] - 1
            dawk_io::extract_file_portion(file_path, start_line, stop_line, output_file)
            start_line = stop_line + 1
        }
       
    }

    # recalculate max workers
    max_workers = 0
    for (worker in nbr_lines_per_worker) {
        if (nbr_lines_per_worker[worker] != 0) {
            max_workers = max_workers + 1
        }
    }

    print "" > "process.sh"
    for (worker in nbr_lines_per_worker) {
        if (nbr_lines_per_worker[worker] != 0) {
            content_to_process = "./workers/"worker"/to_process"

            # move the mapper script to the worker
            dawk_io::copy_file(awk::MAPPER, "./workers/"worker"/mapper_script")
            # move the shuffler to the worker
            dawk_io::copy_file("src/shuffle.awk", "./workers/"worker"/shuffle.awk")
            dawk_io::copy_file(awk::REDUCER, "./workers/"worker"/reducer_script")

            # generate map command
            map_command = "awk -F \"" awk::MAP_FS "\" -f ./workers/"worker"/mapper_script -v output_file=./workers/"worker"/map.result " content_to_process
            shuffle_command = " && awk -F \"" awk::RED_FS "\" -f ./workers/"worker"/shuffle.awk -v MAX_WORKERS="max_workers" -v BASE_PATH=\"./\" -v SHUFFLE_KEY="awk::SHUFFLE_KEY" -v SHUFFLE_SEED=123456 ./workers/"worker"/map.result"
            reduce_command = " && awk -F \"" awk::RED_FS "\" -f ./workers/"worker"/reducer_script -v output_file=./workers/"worker"/red.result ./workers/"worker"/shuffle_partition/data"
            print map_command shuffle_command reduce_command " &" >> "process.sh"
        }
        
    }

    print "wait" >> "process.sh"


    # start awk mappers
    # TODO récupérer le code de retour des processus
    dawk_logging::info("Starting map and shuffle")
    system("bash process.sh")
    dawk_logging::info("map, shuffle and reduce complete")
    dawk_logging::info("start data aggregation")
    # aggregate results into one file
    print "" > "dawk_aggregation.sh"
    aggreg_command = "cat "
    for (worker in nbr_lines_per_worker) {
        if (nbr_lines_per_worker[worker] != 0) {
            aggreg_command = aggreg_command "./workers/"worker"/red.result "
        }
    }
    aggreg_command = aggreg_command " > dawk_result"
    print aggreg_command >> "dawk_aggregation.sh"
    system("bash dawk_aggregation.sh")
    dawk_logging::info("processing complete")

    dawk_io::removedirrec("./workers/")
    dawk_io::removefile("process.sh")
    dawk_io::removefile("dawk_aggregation.sh")
    
}

BEGIN {

    # Check execution mode
    # LOCAL only available
    if (awk::MODE != "local") {
        dawk_logging::error("Execution mode not possible. local is the only option")
        exit 1
    }
    # prepare environment
    current_file_name = ""

    # Check if nbr_workers is correctly set
    # This MUST be specified by the user
    if (awk::NBR_WORKERS == 0 || awk::NBR_WORKERS == "") {
        dawk_logging::error("nbr_worker does not have a correct value. Current value ="awk::NBR_WORKERS"=")
        exit 1
    }

    # check if the shuffle key is indicated. If not, will take $1
    if (awk::SHUFFLE_KEY == "") {
        dawk_logging::warning("shuffle key is not given, will use $1")
        awk::SHUFFLE_KEY = "$1"
    }

    if (awk::MAP_FS != "") {
        if (awk::MAP_FS != FS) {
            dawk_logging::info("map field separator changed by user to " awk::MAP_FS)
        }
    }


    if (awk::RED_FS != FS) {
        dawk_logging::info("red field separator changed by user to " awk::RED_FS)
    }

    # data split output
    BASE_PATH="./"

    # number of lines assigned to each workers
    # nbr_lines_per_worker

    dawk_logging::info("mapper is " awk::MAPPER)
    dawk_logging::info("reducer is " awk::REDUCER)
    dawk_logging::info("shuffle key is " awk::SHUFFLE_KEY)
}

{
    if (current_file_name != FILENAME) {
        process_file(FILENAME, BASE_PATH)
        current_file_name = FILENAME
    }
}
