const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const { createRequire } = require('node:module');

exports.loadExecution = function loadExecution(repo, snapshot) {
  const ui = path.join(repo, 'ui/ops-dashboard');
  const projectRequire = createRequire(path.join(ui, 'package.json'));
  const ts = projectRequire('typescript');
  const sourceRoot = path.join(ui, 'src');
  const boundary = path.join(sourceRoot, 'useSnapshot.ts');
  const cache = new Map();
  const sourceManifest = [];
  const boundaryCalls = [];

  function load(file) {
    if (file === boundary) {
      return { useSnapshot(requestPath) {
        boundaryCalls.push(requestPath);
        if (requestPath !== '/api/execution') throw new Error('Unexpected snapshot path');
        return { snapshot, data: snapshot?.data, loading: false, error: null };
      } };
    }
    if (cache.has(file)) return cache.get(file).exports;
    const source = fs.readFileSync(file, 'utf8');
    sourceManifest.push({ path: file, sha256: crypto.createHash('sha256').update(source).digest('hex') });
    const transpiled = ts.transpileModule(source, {
      fileName: file,
      compilerOptions: {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.CommonJS,
        jsx: ts.JsxEmit.ReactJSX,
        esModuleInterop: true,
      },
    });
    const module = { exports: {} };
    cache.set(file, module);
    function localRequire(specifier) {
      if (!specifier.startsWith('.')) return projectRequire(specifier);
      const base = path.resolve(path.dirname(file), specifier);
      const target = [base, base + '.ts', base + '.tsx', path.join(base, 'index.ts'), path.join(base, 'index.tsx')]
        .find(candidate => fs.existsSync(candidate) && fs.statSync(candidate).isFile());
      if (!target) throw new Error('Cannot resolve ' + specifier + ' from ' + file);
      if (!target.startsWith(sourceRoot + path.sep)) throw new Error('Unexpected source boundary');
      return load(target);
    }
    const wrap = vm.runInThisContext('(function(require,module,exports,__filename,__dirname){\n' + transpiled.outputText + '\n})', { filename: file });
    wrap(localRequire, module, module.exports, file, path.dirname(file));
    return module.exports;
  }
  return {
    Execution: load(path.join(sourceRoot, 'screens/Execution.tsx')).Execution,
    React: projectRequire('react'),
    ReactDOMServer: projectRequire('react-dom/server'),
    sourceManifest,
    boundaryCalls,
    versions: { react: projectRequire('react/package.json').version, reactDom: projectRequire('react-dom/package.json').version, typescript: ts.version },
  };
};
