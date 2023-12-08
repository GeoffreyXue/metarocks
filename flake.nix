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

          buildInputs = with nixpkgs.legacyPackages.${system}; [
            cmake
            clang-tools
            git
            zlib
            bzip2
            snappy
            lz4
            zstd
            gflags
          ];
          
          packages = [
          ];

          # set MACOSX_DEPLOYMENT_TARGET to 12.6
          shellHook = ''
            export MACOSX_DEPLOYMENT_TARGET=12.6
          '';
        };
      });
    };
}