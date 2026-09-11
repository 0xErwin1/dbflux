//! The `db` and `print` script-scope globals.
//!
//! Everything under `db` is defined in JavaScript ([`BOOTSTRAP_SOURCE`]),
//! using standard `Proxy` and `Array` semantics so `.forEach`, `for...of`,
//! `.map`, and `.length` work natively on `find()`/`aggregate()` results at
//! zero extra cost. The only native surface is two functions the engine
//! registers before evaluating the bootstrap: `__dispatch` (one script
//! statement in, one JSON result out or a thrown error) and `__print`
//! (appends to the run's output buffer). No other global is registered —
//! `require`, `import`, `Deno`, `process`, and filesystem/network APIs are
//! simply never reachable from script scope.

/// JavaScript evaluated once per run, before the user's script, to define
/// `db` and `print`. All navigation and array semantics live here; the only
/// calls back into Rust are `__dispatch` and `__print`.
pub const BOOTSTRAP_SOURCE: &str = r#"
(function () {
  var DB_LEVEL_METHODS = ["dropDatabase","stats","dbStats","serverStatus","runCommand","getCollectionNames","listCollections","createCollection"];
  var CURSOR_ONLY = ["hasNext","next","limit","skip","sort","count"];

  function cursorThrow(name) {
    return function () {
      throw new Error(
        "." + name + "() is not supported on script query results: " +
        "find()/aggregate() already return a materialised array — use toArray() or iterate directly."
      );
    };
  }

  function wrapArrayResult(docs) {
    var arr = docs.slice();
    for (var i = 0; i < CURSOR_ONLY.length; i++) {
      Object.defineProperty(arr, CURSOR_ONLY[i], {
        value: cursorThrow(CURSOR_ONLY[i]),
        enumerable: false,
        configurable: true,
      });
    }
    Object.defineProperty(arr, "toArray", {
      value: function () { return arr; },
      enumerable: false,
      configurable: true,
    });
    return arr;
  }

  function invoke(targetKind, targetName, method, args) {
    var resultJson = __dispatch(targetKind, targetName || "", method, JSON.stringify(args));
    var result = JSON.parse(resultJson);
    if (method === "find" || method === "aggregate") {
      return wrapArrayResult(result.documents || []);
    }
    return result;
  }

  function makeContainerProxy(name) {
    return new Proxy({}, {
      get: function (_target, prop) {
        if (typeof prop !== "string") { return undefined; }
        return function () {
          return invoke("container", name, prop, Array.prototype.slice.call(arguments));
        };
      },
    });
  }

  var db = new Proxy({}, {
    get: function (_target, prop) {
      if (typeof prop !== "string") { return undefined; }
      if (DB_LEVEL_METHODS.indexOf(prop) !== -1) {
        return function () {
          return invoke("database", "", prop, Array.prototype.slice.call(arguments));
        };
      }
      return makeContainerProxy(prop);
    },
  });

  globalThis.db = db;
  globalThis.print = function () {
    var parts = [];
    for (var i = 0; i < arguments.length; i++) {
      var a = arguments[i];
      parts.push(typeof a === "string" ? a : JSON.stringify(a));
    }
    __print(parts.join(" "));
  };
})();
"#;
