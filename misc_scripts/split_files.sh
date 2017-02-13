tail -n +2 test.csv | split -l 1000000 - test_
for file in test_*
do
    head -n 1 test.csv > tmp_file
    cat $file >> tmp_file
    mv -f tmp_file $file
done
