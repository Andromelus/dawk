{
    for(i=1; i<=NF; i++) {
        counter[$i] = counter[$i] + 1
    }
}

END {
    for (count in counter) {
        print count " " counter[count] >> output_file
    }
}