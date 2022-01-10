// Author:

/*************************************************************************
 * Copyright (C) 1995-2022, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RRESAMPLEDS
#define ROOT_RRESAMPLEDS

#include "ROOT/RDF/RColumnRegister.hxx"
#include "ROOT/RDF/RColumnCache.hxx"
#include "ROOT/RDF/RColumnCacheReader.hxx"
#include "ROOT/RDF/RColumnReaderRemapper.hxx"
#include "ROOT/RDF/RDefineReader.hxx"
#include "ROOT/RDF/RLoopManager.hxx"
#include "ROOT/RDF/Utils.hxx"
#include "ROOT/RMovingCachedDS.hxx"

#include <map>
#include <memory>

namespace ROOT {

namespace Internal {

namespace RDF {

namespace RDFDetail = ROOT::Detail::RDF;

template <typename Proxied, typename TimeType>
class RResampleDS : public RDFInternal::RMovingCachedDS<Proxied> {
protected:
   std::string fTimeColumn;
   TimeType fResampleStepsize;
   TimeType fResampleFrom;
   TimeType fResampleTo;

   std::unique_ptr<RDFInternal::RColumnCache<TimeType>> fSnapshotTimes;

   std::vector<std::pair<ULong64_t, ULong64_t>> fSourceRanges;
   std::vector<std::unique_ptr<RDFDetail::RColumnReaderBase>> fTimeColumnReaders;
   std::vector<std::map<Long64_t, Long64_t>> fResampleIndices;
   std::vector<Long64_t> fLastStoredSnapshot;

   Long64_t getSnapshotIndex(TimeType time) { return std::floor((time - fResampleFrom) / fResampleStepsize); }

public:
   RResampleDS(std::shared_ptr<Proxied> proxiedPtr, RLoopManager *sourceLoopManager,
               const RDFInternal::RColumnRegister &columnRegister, const std::string &timeColumn,
               TimeType resampleStepsize, TimeType resampleFrom, TimeType resampleTo)
      : RMovingCachedDS<Proxied>(proxiedPtr, sourceLoopManager, columnRegister), fTimeColumn(timeColumn),
        fResampleStepsize(resampleStepsize), fResampleFrom(resampleFrom), fResampleTo(resampleTo),
        fSnapshotTimes(std::make_unique<RDFInternal::RColumnCache<TimeType>>(sourceLoopManager->GetNSlots())),
        fTimeColumnReaders(sourceLoopManager->GetNSlots()), fResampleIndices(sourceLoopManager->GetNSlots()),
        fLastStoredSnapshot(sourceLoopManager->GetNSlots())
   {
   }

   virtual ~RResampleDS() = default;

   virtual std::vector<std::pair<ULong64_t, ULong64_t>> GetEntryRanges()
   {
      if (this->fDataSource) {
         fSourceRanges = this->fDataSource->GetEntryRanges();
      } else {
         if (this->fSourceLoopManager->GetNSlots() > 1) {
            throw std::runtime_error("Multiple slots with an empty data source not implemented.");
         }

         fSourceRanges.clear();

         if (this->fNGetEntryRangesCalled == 0) {
            ULong64_t numberOfEntries = this->fSourceLoopManager->GetNEmptyEntries();
            fSourceRanges.emplace_back(0, numberOfEntries);
         }
      }

      if (this->fNGetEntryRangesCalled == 0) {
         this->fRanges.emplace_back(0, getSnapshotIndex(fResampleTo) + 1);
      }

      // InitSlot is not always called with the correct firstEntry, so init the caches here.
      if (this->fRanges.size() > 0) {
         if (this->fRanges.size() != this->fSourceLoopManager->GetNSlots()) {
            throw std::runtime_error(
               "Number of ranges does not match number of slots, special implementation required.");
         }

         for (int slot = 0; slot < this->fSourceLoopManager->GetNSlots(); slot++) {
            this->fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] =
               static_cast<Long64_t>(fSourceRanges[slot].first) - 1;
            this->fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] =
               static_cast<Long64_t>(fSourceRanges[slot].first) - 1;
            fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()] = -1;

            for (const auto &cache : this->fCaches) {
               cache.second->InitSlot(slot, static_cast<Long64_t>(fSourceRanges[slot].first));
            }

            fTimeColumnReaders[slot] =
               std::make_unique<RColumnCacheReader>(slot, this->fCaches.at(std::string(fTimeColumn)).get());

            fSnapshotTimes->InitSlot(slot, static_cast<Long64_t>(this->fRanges[slot].first));
         }
      }

      this->fNGetEntryRangesCalled++;

      return this->fRanges;
   }

   virtual bool SetEntry(unsigned int slot, ULong64_t entry)
   {
      while (fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()] <
             static_cast<Long64_t>(entry) + this->fEntryOffsetLimit.second) {
         this->fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()]++;

         if (!this->LoadEntry(slot, this->fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()])) {
            fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()]++;

            TimeType snapshotTime =
               fResampleFrom + fResampleStepsize * fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()];

            fSnapshotTimes->LoadValue(slot, snapshotTime);

            fResampleIndices[slot][fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()]] =
               this->fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()];
         } else {
            if (this->fProxiedPtr->CheckFilters(
                   slot, this->fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()])) {
               for (const auto &cache : this->fCaches) {
                  cache.second->Load(slot, this->fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()]);
               }
               this->fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()]++;

               Long64_t lastLoadedEntry = this->fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()];
               auto entryTime = fTimeColumnReaders[slot]->template Get<TimeType>(lastLoadedEntry);

               if (this->fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] ==
                      static_cast<Long64_t>(fSourceRanges[slot].first) - 1 &&
                   fResampleFrom < entryTime) {
                  throw std::runtime_error("First entry after start of resampling.");
               }

               while (fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()] <
                         getSnapshotIndex(entryTime) &&
                      fResampleFrom + fResampleStepsize *
                                         (fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()] + 1) <
                         entryTime) {
                  fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()]++;

                  TimeType snapshotTime =
                     fResampleFrom +
                     fResampleStepsize * fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()];

                  fSnapshotTimes->LoadValue(slot, snapshotTime);

                  fResampleIndices[slot][fLastStoredSnapshot[slot * RDFInternal::CacheLineStep<Long64_t>()]] =
                     this->fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] - 1;
               }
            }
         }
      }

      Long64_t firstUsedIndex = fResampleIndices[slot][static_cast<Long64_t>(entry) + this->fEntryOffsetLimit.first];
      for (const auto &cache : this->fCaches) {
         cache.second->PurgeTill(slot, firstUsedIndex - 1);
      }
      fSnapshotTimes->PurgeTill(slot, static_cast<Long64_t>(entry) + this->fEntryOffsetLimit.first - 1);

      return true;
   }

   virtual std::string GetLabel() { return "RResampleDS"; }

   std::unique_ptr<RDFDetail::RColumnReaderBase>
   GetColumnReaders(unsigned int slot, std::string_view name, const std::type_info &tid) final
   {
      (void)tid;

      if (name == fTimeColumn) {
         return std::make_unique<RColumnCacheReader>(slot, fSnapshotTimes.get());
      }

      if (this->fCaches.count(std::string(name)) > 0) {
         auto directReader = std::make_unique<RColumnCacheReader>(slot, this->fCaches.at(std::string(name)).get());

         auto remapper = [slot, &fResampleIndices = fResampleIndices](Long64_t entry) {
            return fResampleIndices[slot][entry];
         };

         return std::make_unique<RColumReaderRemapper<decltype(remapper)>>(std::move(directReader), remapper);
      } else {
         throw std::runtime_error("Column not cached.");
      }
   }
};

template <typename Proxied, typename TimeType, typename... ColumnTypes>
static std::unique_ptr<RResampleDS<Proxied, TimeType>>
MakeRResampleDS(std::shared_ptr<Proxied> proxiedPtr, RLoopManager *sourceLoopManager,
                const RDFInternal::RColumnRegister &colRegister, const std::string &timeColumn,
                TimeType resampleStepsize, TimeType resampleFrom, TimeType resampleTo, ColumnNames_t &columns,
                const std::vector<std::string> &columnTypes)
{
   auto ds = std::make_unique<RResampleDS<Proxied, TimeType>>(proxiedPtr, sourceLoopManager, colRegister, timeColumn,
                                                              resampleStepsize, resampleFrom, resampleTo);
   ds->template Setup<ColumnTypes...>(columns, columnTypes);
   return ds;
}

} // namespace RDF

} // namespace Internal

} // namespace ROOT

#endif