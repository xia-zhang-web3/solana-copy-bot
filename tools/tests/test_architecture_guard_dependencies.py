from architecture_guard_fixtures.harness import GuardCase


class Dependencies(GuardCase):
    def test_app_specs_alias_table_target_build_dev(self):
        variants = (
            '[dependencies]\ncopybot-storage-core={path="../storage-core"}\n',
            '[dev-dependencies]\nalias={package="copybot-storage-core", path="../storage-core"}\n',
            '[build-dependencies.alias]\npackage="copybot-storage-core"\npath="../storage-core"\n',
            '[target.\'cfg(unix)\'.dependencies]\nalias={package="copybot-storage-core", path="../storage-core"}\n',
        )
        for addition in variants:
            with self.subTest(addition=addition):
                self.package('app', addition)
                self.lock()
                self.pair(expected=1, contains=('direct dependency spec',))

    def test_app_spec_changed_and_workspace_alias(self):
        self.package('app', '[dependencies]\ncopybot-storage-core={path="../storage-core"}\n')
        self.lock()
        self.commit()
        self.package('app', '[dependencies]\ncopybot-storage-core={path="../storage-core", optional=true}\n')
        self.lock()
        self.pair(expected=1, contains=('direct dependency spec',))
        self.commit()
        self.append('Cargo.toml', '[workspace.dependencies]\nalias={package="copybot-storage-core", path="crates/storage-core"}\n')
        self.pair(expected=1, contains=('workspace dependency spec',))

    def test_forbidden_direct_and_transitive_edges(self):
        self.package('shadow')
        self.package('bridge', '[dependencies]\ncopybot-shadow={path="../shadow"}\n')
        self.lock()
        self.commit()
        for section in ('dependencies', 'dev-dependencies', 'build-dependencies', 'target.\'cfg(unix)\'.dependencies'):
            with self.subTest(section=section):
                self.package('operators', f'[{section}]\nalias={{package="copybot-shadow", path="../shadow"}}\n')
                self.lock()
                self.pair(expected=1, contains=('declares forbidden operator dependency', 'pulls forbidden transitive dependency'))
        self.package('operators', '[dependencies]\ncopybot-bridge={path="../bridge"}\n')
        self.lock()
        self.pair(expected=1, contains=('pulls forbidden transitive dependency: copybot-shadow',))

    def test_target_table_and_operator_graph_exception(self):
        self.package('tonic')
        path = self.root / 'crates/tonic/Cargo.toml'
        path.write_text(path.read_text().replace('copybot-tonic', 'tonic'))
        self.package('operators', '[dependencies]\nalias={package="tonic", path="../tonic"}\n')
        self.lock()
        self.pair()
        self.package('storage-core', '[target.\'cfg(unix)\'.dev-dependencies.alias]\npackage="tonic"\npath="../tonic"\n')
        self.lock()
        self.pair(expected=1, contains=('forbidden operator dependency: tonic', 'forbidden transitive dependency: tonic'))

    def test_duplicate_binaries_and_unregistered_bin(self):
        for crate in ('operators', 'storage-core'):
            self.write(f'crates/{crate}/src/bin/duplicate.rs', 'fn main() {}\n')
        self.pair(expected=1, contains=('duplicate workspace bin name: duplicate count=2',))
        self.package('operators', 'autobins=false\n')
        self.pair(expected=1, contains=('duplicate workspace bin name',))

    def test_required_profile_and_policy_command(self):
        self.write('Cargo.toml', '[workspace]\nmembers=["crates/*"]\nresolver="2"\n')
        self.pair(expected=1, contains=('missing [profile.operator-release]',))
        for path in ('BUILD_POLICY.md', 'BUILD_REFACTOR_ROADMAP.md', 'ARTIFACT_DEPLOY.md'):
            self.write(path, 'Policy\n')
        self.pair(expected=1, contains=('architecture docs do not mention',))

    def test_allowed_app_signer_dependency(self):
        self.package('base64')
        path = self.root / 'crates/base64/Cargo.toml'
        path.write_text(path.read_text().replace('copybot-base64', 'base64').replace('0.1.0', '0.22.1'))
        self.append('Cargo.toml', '[patch.crates-io]\nbase64={path="crates/base64"}\n')
        # This positive control needs no registry cache or network at all.
        self.env['CARGO_HOME'] = str(self.root / '.git/empty-cargo-home')
        self.append('crates/app/Cargo.toml', '[dependencies]\nbase64="0.22.1"\n')
        self.lock()
        self.pair()

    def test_quoted_direct_tables_and_workspace_specs(self):
        self.package('shadow')
        self.package('operators', '[dependencies."copybot-shadow"]\npath="../shadow"\n')
        self.lock()
        self.pair(expected=1, contains=('declares forbidden operator dependency',))
        self.package('operators', "[dev-dependencies.'copybot-shadow']\npath=\"../shadow\"\n")
        self.lock()
        self.pair(expected=1, contains=('declares forbidden operator dependency',))
        self.append('Cargo.toml', '[workspace.dependencies.alias]\npackage="copybot-shadow"\npath="crates/shadow"\n')
        self.pair(expected=1, contains=('workspace dependency spec',))
