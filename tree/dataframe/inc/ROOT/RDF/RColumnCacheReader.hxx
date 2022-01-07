// Author: Enrico Guiraud CERN 09/2020

/*************************************************************************
 * Copyright (C) 1995-2020, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RCOLUMNCACHEREADER
#define ROOT_RDF_RCOLUMNCACHEREADER

#include "RColumnReaderBase.hxx"
#include "RColumnCacheBase.hxx"
#include <Rtypes.h> // Long64_t

#include <limits>
#include <type_traits>

namespace ROOT {
namespace Internal {
namespace RDF {

namespace RDFDetail = ROOT::Detail::RDF;

class RColumnCacheReader final : public ROOT::Detail::RDF::RColumnReaderBase {
   int fSlot;

   RColumnCacheBase *fCache;

   void *GetImpl(Long64_t entry) final { return fCache->Get(fSlot, entry); }

public:
   RColumnCacheReader(unsigned int slot, RColumnCacheBase *cache) : fSlot(slot), fCache(cache) {}
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT

#endif
