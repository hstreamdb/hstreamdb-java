#!/usr/bin/env sh
echo "Running patchelf on files matching *protoc*.exe..."
nix-shell -p patchelf --run "find $HOME/.gradle/caches -type f -name '*protoc*.exe' -print0 | xargs -0 -I{} sh -c 'echo \"Processing {}...\"; patchelf --set-interpreter \"\$(cat \$NIX_CC/nix-support/dynamic-linker)\" {}; echo \"Finished processing {}\"'"
echo "Finished running patchelf."
