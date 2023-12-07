{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-22.11";
    systems.url = "github:nix-systems/default";
  };

  outputs = { self, systems, nixpkgs } @ inputs:
    let
      eachSystem = nixpkgs.lib.genAttrs (import systems);
    in
    {
       devShells = eachSystem (system: {
        default = with nixpkgs.legacyPackages.${system}; mkShell {
          name = "rocksdb-dev";

          xcbuildFlags = [
           "MACOSX_DEPLOYMENT_TARGET=10.12"
          ];

          packages = [
            nix

            # rocksdb deps
            zlib
            bzip2
            snappy
            lz4
            zstd
            gflags

            cmake
            clang-tools
          ];
        };
      });
    };
}