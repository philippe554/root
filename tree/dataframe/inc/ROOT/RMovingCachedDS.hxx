// Author:

/*************************************************************************
 * Copyright (C) 1995-2018, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RMOVINGCACHEDDS
#define ROOT_RMOVINGCACHEDDS

#include "ROOT/RProxyDS.hxx"
#include "ROOT/RDF/RColumnRegister.hxx"
#include "ROOT/RDF/RColumnCache.hxx"
#include "ROOT/RDF/RColumnCacheReader.hxx"
#include "ROOT/RDF/RDefineReader.hxx"
#include "ROOT/RDF/RLoopManager.hxx"
#include "ROOT/RDF/Utils.hxx"

#include <map>

namespace ROOT {

namespace Internal {

namespace RDF {

namespace RDFDetail = ROOT::Detail::RDF;
namespace RDFInternal = ROOT::Internal::RDF;

template <typename SourceDataFrame, typename Proxied>
class RMovingCachedDS final : public RDFInternal::RProxyDS {
protected:
   SourceDataFrame fSourceDataFrame;
   std::shared_ptr<Proxied> fProxiedPtr;
   RDFInternal::RColumnRegister fColumnRegister;

   std::vector<std::pair<ULong64_t, ULong64_t>> fRanges; // only used if source is an empty dataframe
   int fNGetEntryRangesCalled = 0;
   std::pair<int, int> fEntryOffsetLimit = {0, 0};

   ROOT::RDF::ColumnNames_t fColumnNames;
   std::map<std::string, std::string> fColumnTypes;

   std::map<std::string, std::unique_ptr<RDFInternal::RColumnCacheBase>> fCaches;
   std::vector<Long64_t> fSourceLoadedEntries;
   std::vector<Long64_t> fLoadedEntries;

public:
   RMovingCachedDS(SourceDataFrame &sourceDataFrame, std::shared_ptr<Proxied> proxiedPtr,
                   RLoopManager *sourceLoopManager, const RDFInternal::RColumnRegister &columnRegister)
      : RProxyDS(sourceLoopManager), fSourceDataFrame(sourceDataFrame), fProxiedPtr(proxiedPtr),
        fColumnRegister(columnRegister),
        fSourceLoadedEntries(sourceLoopManager->GetNSlots() * RDFInternal::CacheLineStep<Long64_t>()),
        fLoadedEntries(sourceLoopManager->GetNSlots() * RDFInternal::CacheLineStep<Long64_t>())
   {
   }

   virtual ~RMovingCachedDS() = default;

   template <typename... ColumnTypes>
   void Setup(ColumnNames_t &columns)
   {
      fColumnNames = columns;
      for (const auto &columnName : fColumnNames) {
         fColumnTypes[columnName] = fSourceDataFrame.GetColumnType(columnName);
      }

      int i = 0;
      int expander[] = {(SetupCache<ColumnTypes>(columns[i]), ++i)..., 0};
      (void)expander;
   }

   template <typename T>
   void SetupCache(const std::string &name)
   {
      std::vector<std::unique_ptr<RDFDetail::RColumnReaderBase>> readers;

      if (fColumnRegister.HasName(name)) {
         auto define = fColumnRegister.GetColumns().at(name.data());

         for (int slot = 0; slot < fSourceLoopManager->GetNSlots(); slot++) {
            auto reader = std::make_unique<RDFInternal::RDefineReader>(slot, *define, typeid(T));
            readers.push_back(std::move(reader));
         }
      } else if (fDataSource) {
         if (fDataSource->HasColumn(name)) {
            for (int slot = 0; slot < fSourceLoopManager->GetNSlots(); slot++) {
               auto reader = fDataSource->GetColumnReaders(slot, name, typeid(T));
               readers.push_back(std::move(reader));
            }
         }
      }

      if (readers.size() != fSourceLoopManager->GetNSlots()) {
         throw std::runtime_error("Number of readers for a cache do not match the number of slots.");
      }

      fCaches[name] = std::make_unique<RColumnCache<T>>(std::move(readers));
   }

   void AddEntryOffsetLimit(const std::pair<int, int> &entryOffsetLimit)
   {
      fEntryOffsetLimit.first = std::min(fEntryOffsetLimit.first, entryOffsetLimit.first);
      fEntryOffsetLimit.second = std::max(fEntryOffsetLimit.second, entryOffsetLimit.second);
   }

   std::string GetTypeName(std::string_view colName) const
   {
      if (fColumnTypes.count(colName.data()) == 0) {
         throw std::runtime_error(std::string("Column not found in RMovingCachedDS: ") + colName.data());
      }
      return fColumnTypes.at(colName.data());
   }

   const std::vector<std::string> &GetColumnNames() const { return fColumnNames; }

   bool HasColumn(std::string_view colName) const
   {
      return fColumnNames.end() != std::find(fColumnNames.begin(), fColumnNames.end(), colName);
   }

   virtual std::vector<std::pair<ULong64_t, ULong64_t>> GetEntryRanges()
   {
      if (fDataSource) {
         fRanges = fDataSource->GetEntryRanges();

         for (auto &range : fRanges) {
            range.first += static_cast<ULong64_t>(-fEntryOffsetLimit.first);
            range.second -= static_cast<ULong64_t>(fEntryOffsetLimit.second);
         }
      } else {
         if (fSourceLoopManager->GetNSlots() > 1) {
            throw std::runtime_error("Multiple slots with an empty data source not implemented.");
         }

         fRanges.clear();

         if (fNGetEntryRangesCalled == 0) {
            ULong64_t numberOfEntries = fSourceLoopManager->GetNEmptyEntries();
            fRanges.emplace_back(static_cast<ULong64_t>(-fEntryOffsetLimit.first),
                                 numberOfEntries - static_cast<ULong64_t>(fEntryOffsetLimit.second));
         }
      }

      // InitSlot is not always called with the correct firstEntry, so init the caches here.
      if (fRanges.size() > 0) {
         if (fRanges.size() != fSourceLoopManager->GetNSlots()) {
            throw std::runtime_error(
               "Number of ranges does not match number of slots, special implementation required.");
         }

         for (int slot = 0; slot < fSourceLoopManager->GetNSlots(); slot++) {
            fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] =
               static_cast<Long64_t>(fRanges[slot].first) + fEntryOffsetLimit.first - 1;
            fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] =
               static_cast<Long64_t>(fRanges[slot].first) + fEntryOffsetLimit.first - 1;

            for (const auto &cache : fCaches) {
               cache.second->InitSlot(slot, static_cast<Long64_t>(fRanges[slot].first) + fEntryOffsetLimit.first);
            }
         }
      }

      fNGetEntryRangesCalled++;

      return fRanges;
   }

   virtual bool SetEntry(unsigned int slot, ULong64_t entry)
   {
      while (fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] - fEntryOffsetLimit.second <
             static_cast<Long64_t>(entry)) {
         fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()]++;

         if (fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()] >= fRanges[slot].second) {
            return false;
         }

         if (!LoadEntry(slot, fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()])) {
            return false;
         }

         if (fProxiedPtr->CheckFilters(slot, fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()])) {
            for (const auto &cache : fCaches) {
               cache.second->Load(slot, fSourceLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()]);
            }
            fLoadedEntries[slot * RDFInternal::CacheLineStep<Long64_t>()]++;
         }
      }

      for (const auto &cache : fCaches) {
         cache.second->PurgeTill(slot, static_cast<Long64_t>(entry) + fEntryOffsetLimit.first - 1);
      }

      return true;
   }

   virtual void InitialiseDerived() { fNGetEntryRangesCalled = 0; }

   virtual void FinaliseSlotDerived(unsigned int slot)
   {
      for (const auto &cache : fCaches) {
         cache.second->FinaliseSlot(slot);
      }
   }

   virtual std::string GetLabel() { return "RMovingCachedDS"; }

   virtual std::unique_ptr<ROOT::Detail::RDF::RColumnReaderBase>
   GetColumnReaders(unsigned int slot, std::string_view name, const std::type_info &tid)
   {
      (void)tid;

      if (fCaches.count(std::string(name)) > 0) {
         return std::make_unique<RColumnCacheReader>(slot, fCaches.at(std::string(name)).get());
      } else {
         throw std::runtime_error("Column not cached.");
      }
   }
};

template <typename SourceDataFrame, typename Proxied, typename... ColumnTypes>
static std::unique_ptr<RMovingCachedDS<SourceDataFrame, Proxied>>
MakeRMovingCachedDS(SourceDataFrame &sourceDataFrame, std::shared_ptr<Proxied> proxiedPtr,
                    RLoopManager *sourceLoopManager, const RDFInternal::RColumnRegister &colRegister,
                    ColumnNames_t &columns)
{
   auto ds = std::make_unique<RMovingCachedDS<SourceDataFrame, Proxied>>(sourceDataFrame, proxiedPtr, sourceLoopManager,
                                                                         colRegister);
   ds->template Setup<ColumnTypes...>(columns);
   return ds;
}

} // namespace RDF

} // namespace Internal

} // namespace ROOT

#endif