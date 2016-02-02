var assert = require("assert");
var discountCalculator = require("../discount");
describe("discount Calculator", function(){
it("should have discount function", function(){
assert.equal(discountCalculator.discount instanceof Function, true,
"typeof discount: " + typeof discountCalculator.discount);
});
});

describe("discount function", function(){
it("discount should return 0 with no args ", function(){
var discount = discountCalculator.discount();
assert.equal(discount, 0, discount + "");
});
});

