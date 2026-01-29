{ pkgs ? import <nixpkgs> {} }:

let
  dbflux = import ./default.nix { inherit pkgs; };
in
dbflux.shell
