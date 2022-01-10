// Author: Enrico Guiraud CERN 09/2020

/*************************************************************************
 * Copyright (C) 1995-2020, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RCOLUMNCACHE
#define ROOT_RDF_RCOLUMNCACHE

#include <Rtypes.h> // Long64_t

#include "RColumnCacheBase.hxx"
#include "RColumnReaderBase.hxx"

#include <deque>

namespace ROOT {
namespace Internal {
namespace RDF {

namespace RDFDetail = ROOT::Detail::RDF;
namespace RDFInternal = ROOT::Internal::RDF;

template <typename T>
class RColumnCache : public RColumnCacheBase {
private:
   std::vector<std::unique_ptr<RDFDetail::RColumnReaderBase>> fReaders;

   std::vector<std::unique_ptr<std::deque<T>>> fCaches;

   std::vector<Long64_t> fCurrentFirstEntries;

public:
   RColumnCache(std::vector<std::unique_ptr<RDFDetail::RColumnReaderBase>> &&readers)
      : fReaders(std::move(readers)), fCaches(fReaders.size()),
        fCurrentFirstEntries(fReaders.size() * RDFInternal::CacheLineStep<Long64_t>())
   {
   }

   RColumnCache(int nSlots) : fCaches(nSlots), fCurrentFirstEntries(nSlots * RDFInternal::CacheLineStep<Long64_t>()) {}

   virtual ~RColumnCache(){};

   void InitSlot(unsigned int slot, Long64_t startEntry)
   {
      fCaches[slot] = std::make_unique<std::deque<T>>();
      fCurrentFirstEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] = startEntry;
   }

   void FinaliseSlot(unsigned int slot) { fCaches[slot]->clear(); }

   void *Get(int slot, Long64_t entry)
   {
      Long64_t index = entry - fCurrentFirstEntries[slot * RDFInternal::CacheLineStep<Long64_t>()];

      if (index < 0 || index >= fCaches[slot]->size()) {
         throw std::runtime_error(std::string("RColumnCache: trying to access value outside cache range: ") +
                                  std::to_string(index));
      }

      return static_cast<void *>(&fCaches[slot]->at(index));
   }

   void Load(int slot, Long64_t entrySource) { fCaches[slot]->push_back(fReaders[slot]->template Get<T>(entrySource)); }

   void LoadValue(int slot, const T &value) { fCaches[slot]->push_back(value); }

   void PurgeTill(int slot, Long64_t entry)
   {
      while (fCaches[slot]->size() > 0 &&
             fCurrentFirstEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] <= entry) {
         fCaches[slot]->pop_front();
         fCurrentFirstEntries[slot * RDFInternal::CacheLineStep<Long64_t>()]++;
      }

      if (fCurrentFirstEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] <= entry) {
         throw std::runtime_error("RColumnCache: trying to purge more values than possible.");
      }
   }

   std::pair<Long64_t, Long64_t> GetStoredRange(int slot) const
   {
      return {fCurrentFirstEntries[slot * RDFInternal::CacheLineStep<Long64_t>()],
              fCurrentFirstEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] + fCaches[slot]->size()};
   }
};

} // namespace RDF
} // namespace Internal
} // namespace ROOT

#endif
