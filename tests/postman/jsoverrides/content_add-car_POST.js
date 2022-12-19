var axios = require('axios');
var fs = require('fs');
var data = fs.createReadStream("/root/postman-codegen-tests/example.car")

var config = {
  method: 'post',
  url: 'http://localhost:3004/content/add-car?ignore-dupes=&filename=/testfile',
  headers: { 
    'Content-Type': 'application/json', 
    'Content-Length': 188,
    'Accept': 'application/json', 
    'Authorization': 'Bearer APIKEY'
  },
  data : data
};

axios(config)
.then(function (response) {
  console.log(JSON.stringify(response.data));
})
.catch(function (error) {
  throw(error);
});
