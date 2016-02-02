var assert = require("assert");
var discountCalculator = require("../discount");
describe("discount Calculator", function(){
it("should have discount function", function(){
assert.equal(discountCalculator.discount instanceof Function, true,
"typeof discount: " + typeof discountCalculator.discount);
});
});
