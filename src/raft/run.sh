for i in {1..100}
do

if [ $i == 1 ]; then
  go test > out.txt
else
  go test >> out.txt
fi
done
