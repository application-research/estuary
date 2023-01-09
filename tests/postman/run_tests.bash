set -eu

rm -f jstests/*
mkdir -p jstests

node index.js

cp jsoverrides/* jstests/

sed -i jstests/* -e 's/console.log(error)/throw(error)/' -e "s/APIKEY/$APIKEY/"



fails=0
success=0
total=0

set +e

for test_script in $(ls jstests/*|grep -v miner| grep -v deal); do
  total=$((total+1))

  node $test_script 2>&1 > /dev/null
  if [ $? -eq 0 ]; then
    success=$((success+1))
  else
    echo "[$test_script]($test_script)" failed
    echo
    fails=$((fails+1))
  fi

done


echo "Total Successes" $success
echo "Total Fails" $fails
echo "Total Tests" $total


if [ $success -ne $total ]; then
	exit 1
fi

if [ $fails -gt  0 ]; then
	exit 1
fi
