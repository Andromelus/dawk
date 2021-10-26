{
    for(i=1; i<=NF; i++) {
        counter[$i] = counter[$i] + 1
    }
}
END {
    for (key in counter) {
        print key " "counter[key]
    }
}