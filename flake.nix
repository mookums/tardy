{
  description = "an asynchronous runtime for Zig";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/release-24.11";
    iguana.url = "github:mookums/iguana";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      nixpkgs,
      iguana,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        iguanaLib = iguana.lib.${system};
      in
      {
        devShells.default = iguanaLib.mkShell {
          zigVersion = "master";
          withZls = true;

          extraPackages = with pkgs; [
            openssl
            inetutils
            wrk
          ];
        };
      }
    );
}
