{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

    flake-parts.url = "github:hercules-ci/flake-parts";

    naersk = {
      url = "github:nix-community/naersk/master";
      inputs.nixpkgs.follows = "nixpkgs";
    };

    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = inputs @ {
    self,
    nixpkgs,
    flake-parts,
    naersk,
    fenix,
  }:
    inputs.flake-parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux"];

      perSystem = {
        inputs',
        config,
        system,
        pkgs,
        ...
      }: let
        pkgs = (import nixpkgs) {
          inherit system;
        };

        toolchain = fenix.packages."${system}".fromToolchainFile {
          file = ./rust-toolchain.toml;
          sha256 = "sha256-fZ3c9lkVJwYNKcN69GpNsFbfG6oprNgYe6SZRYJ2HYo=";
          # sha256 = pkgs.lib.fakeHash;
        };

        naersk' = naersk.lib.${system}.override {
          cargo = toolchain;
          rustc = toolchain;
        };
      in {
        formatter = pkgs.alejandra;

        devShells.default = with pkgs;
          mkShell ({
            buildInputs = [
              toolchain
              cargo-expand
              cargo-chef
              sqlx-cli
              # tokio-console

              protobuf
              redis
            ];

            RUST_SRC_PATH = "${toolchain}/lib/rustlib/src/rust/library";
            PROTOC = "${protobuf}/bin/protoc";
            PROTOC_INCLUDE = "${protobuf}/include";
            # SQLX_OFFLINE="true";
          });
      };
    };
}
