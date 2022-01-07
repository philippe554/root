// Author: Enrico Guiraud CERN 09/2020

/*************************************************************************
 * Copyright (C) 1995-2020, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RCOLUMNCACHEBASE
#define ROOT_RDF_RCOLUMNCACHEBASE

#include <Rtypes.h> // Long64_t

namespace ROOT {
namespace Internal {
namespace RDF {

class RColumnCacheBase {

public:
   virtual ~RColumnCacheBase(){};

   virtual void InitSlot(unsigned int slot, Long64_t startEntry) = 0;
   virtual void FinaliseSlot(unsigned int slot) = 0;

   virtual void *Get(int slot, Long64_t entry) = 0;

   virtual void Load(int slot, Long64_t entry) = 0;

   virtual void PurgeTill(int slot, Long64_t entry) = 0;

   virtual std::pair<Long64_t, Long64_t> GetStoredRange(int slot) const = 0;
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT

#endif
