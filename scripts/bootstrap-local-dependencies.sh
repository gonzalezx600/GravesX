#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

sha256sum -c <<'CHECKSUMS'
3fdf204b15ae5042d6f134cd57706df1a37c88c0c3a00448cf7e0f6afd7ea2e8  libs/authlib-7.0.61-graves.jar
dcf9a88843f7cc909daf0a1d2f6c3ec5fd92fa7b00abcf6b9641fc79b53b156c  libs/libby-2.0.1-graves2.jar
102ac41e84587b11d2f1886c5341cc01e0a3acc2544f705834fb1da3d9c8b450  libs/playernpc-2023.6.jar
300e6a32226af59c3f280a6c3203a88e21f981057fdb8bb8f5a7661d50e9c3a3  libs/FurnitureEngine-3.3.jar
CHECKSUMS

install='org.apache.maven.plugins:maven-install-plugin:3.1.4:install-file'

mvn -B -ntp "$install" \
  -Dfile=libs/authlib-7.0.61-graves.jar \
  -DgroupId=com.mojang -DartifactId=authlib \
  -Dversion=7.0.61-graves -Dpackaging=jar -DgeneratePom=true

mvn -B -ntp "$install" \
  -Dfile=libs/libby-2.0.1-graves2.jar \
  -DgroupId=com.alessiodp -DartifactId=libby \
  -Dversion=2.0.1-graves2 -Dpackaging=jar -DgeneratePom=true

mvn -B -ntp "$install" \
  -Dfile=libs/playernpc-2023.6.jar \
  -DgroupId=dev.sergiferry -DartifactId=PlayerNPC \
  -Dversion=2023.6 -Dpackaging=jar -DgeneratePom=true

mvn -B -ntp "$install" \
  -Dfile=libs/FurnitureEngine-3.3.jar \
  -DgroupId=com.mira -DartifactId=furnitureengine \
  -Dversion=3.3 -Dpackaging=jar -DgeneratePom=true
