var gm = require('gm')
  , resizeX = 343
  , resizeY = 257

gm('tmp/znak.jpg')
.sepia()
.resize(resizeX, resizeY)
.autoOrient()
.write(response, function (err) {
  if (err) console.log(err);
});