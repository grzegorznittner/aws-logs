/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner
 */
const { dayAgoSuffix, deleteRawData }  = require("../lambdas/cleanup");
const assert = require("assert");

describe("check dayAgoSuffix method", function() {
  it("check date 1", function() {
    const year='2019';
    const month='02';
    const day='24';
    const time = new Date(parseInt(year, 10), parseInt(month, 10)-1, parseInt(day, 10), 2, 30, 45);
    assert.equal(dayAgoSuffix(time, 3), "/year=2019/month=02/day=21");
  });

  it("check date 2", function() {
    const year='2020';
    const month='01';
    const day='3';
    const time = new Date(parseInt(year, 10), parseInt(month, 10)-1, parseInt(day, 10), 2, 30, 45);
    assert.equal(dayAgoSuffix(time, 3), "/year=2019/month=12/day=31");
  });

  it("check date 3", function() {
    const year='2020';
    const month='03';
    const day='01';
    const time = new Date(parseInt(year, 10), parseInt(month, 10)-1, parseInt(day, 10), 2, 30, 45);
    assert.equal(dayAgoSuffix(time, 3), "/year=2020/month=02/day=27");
  });

  it("check date 4", function() {
    const year='2020';
    const month='03';
    const day='12';
    const time = new Date(parseInt(year, 10), parseInt(month, 10)-1, parseInt(day, 10), 1, 20, 45);
    assert.equal(dayAgoSuffix(time, 3), "/year=2020/month=03/day=09");
  });
});
