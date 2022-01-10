// Author:

/*************************************************************************
 * Copyright (C) 1995-2022, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_INTERNAL_RDF_RCOLUMNREADERREMAPPER
#define ROOT_INTERNAL_RDF_RCOLUMNREADERREMAPPER

#include "ROOT/RDF/RColumnReaderBase.hxx"

#include <memory>
#include <Rtypes.h>

namespace ROOT {
namespace Detail {
namespace RDF {

template <class F>
class RColumReaderRemapper : public RColumnReaderBase {
private:
   std::unique_ptr<RColumnReaderBase> fRedirectColumnReader;

   F fRemapper;

public:
   void *GetImpl(Long64_t entry) final { return fRedirectColumnReader->GetImpl(fRemapper(entry)); }

   RColumReaderRemapper(std::unique_ptr<RColumnReaderBase> redirectColumnReader, F remapper)
      : fRedirectColumnReader(std::move(redirectColumnReader)), fRemapper(remapper)
   {
   }
};

} // namespace RDF
} // namespace Detail
} // namespace ROOT

#endif
