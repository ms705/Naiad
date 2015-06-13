#!/bin/sh

# Grab certificates necessary for NuGet
mozroots --import --sync

# To build in Debug mode. Binaries will be placed in $PROJ/bin/Debug.
xbuild /p:Configuration="MonoDebug"
echo "---------------------------------------------"
echo "Mono Debug build done. For a release build, change build_mono.sh"
echo "and uncomment the \"MonoRelease\" configuration line."
echo "---------------------------------------------"

# To build in Release mode. Binaries will be placed in $PROJ/bin/Release.
#xbuild /p:Configuration="MonoRelease"
