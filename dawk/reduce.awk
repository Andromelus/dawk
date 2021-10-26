{
    count[$1] = count[$1] + $2
}
END {
    for (word in count) {
        print word FS count[word] >> output_file
    }
}