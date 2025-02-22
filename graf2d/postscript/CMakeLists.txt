# Copyright (C) 1995-2019, Rene Brun and Fons Rademakers.
# All rights reserved.
#
# For the licensing terms see $ROOTSYS/LICENSE.
# For the list of contributors see $ROOTSYS/README/CREDITS.

############################################################################
# CMakeLists.txt file for building ROOT graf2d/postcript package
# @author Pere Mato, CERN
############################################################################

ROOT_STANDARD_LIBRARY_PACKAGE(Postscript
  HEADERS
    TImageDump.h
    TPDF.h
    TPostScript.h
    TSVG.h
    TTeXDump.h
  SOURCES
    src/TImageDump.cxx
    src/TPDF.cxx
    src/TPostScript.cxx
    src/TSVG.cxx
    src/TTeXDump.cxx
  DICTIONARY_OPTIONS
    -writeEmptyRootPCM
  LIBRARIES
    ZLIB::ZLIB
    mathtext
  DEPENDENCIES
    Graf
)

check_cxx_compiler_flag(-Wbidirectional=none GCC_HAS_BIDIRECTIONAL_FLAG)
if(GCC_HAS_BIDIRECTIONAL_FLAG)
    set_source_files_properties(src/TPostScript.cxx COMPILE_FLAGS "-Wbidirectional=none")
endif()
