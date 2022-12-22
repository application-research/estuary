var axios = require('axios');

var config = {
  method: 'delete',
  url: 'http://localhost:3004/pinning/pins/2',
  headers: { 
    'Accept': 'application/json', 
    'Authorization': 'Bearer APIKEY'
  }
};

axios(config)
.then(function (response) {
  console.log(JSON.stringify(response.data));
})
.catch(function (error) {
  throw(error);
});
