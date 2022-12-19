set -eu

export APIKEY=$(cat APIKEY)

#curl https://raw.githubusercontent.com/application-research/estuary/dev/docs/swagger.json -o swagger.json

rm -f jstests/*

node index.js

cp jsoverrides/* jstests/

sed -i jstests/* -e 's/console.log(error)/throw(error)/' -e "s/APIKEY/$APIKEY/"



fails=0
success=0
total=0

set +e

for test_script in $( ls jstests/*|grep -v DELETE|grep -v miner| grep -v deal ; ls jstests/*|grep DELETE;); do
  total=$((total+1))

  node $test_script > /dev/null
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
