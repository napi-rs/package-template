import lib from "./index.js";

const { plus100 } = lib;

console.assert(plus100(0) === 100, "Simple test failed");

console.info("Simple test passed");
