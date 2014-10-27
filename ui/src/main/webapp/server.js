var express = require('express');
var app = express();

app.use(express.static(__dirname + '/dist'));

app.get('/api/config', function(req, res){
  res.send(undefined);
});


app.listen(process.env.PORT || 8080);

console.log('listening on port ' + (process.env.PORT || 8080));