// Author:

/*************************************************************************
 * Copyright (C) 1995-2022, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RPROXYDS
#define ROOT_RPROXYDS

#include "TChain.h"
#include "TChainElement.h"
#include "TEntryList.h"
#include "TROOT.h"
#include "TTree.h"
#include "TTreeReader.h"
#include "ROOT/InternalTreeUtils.hxx"

#include "ROOT/RDataSource.hxx"
#include "ROOT/RDF/RDefineReader.hxx"
#include "ROOT/RDF/RLoopManager.hxx"

namespace ROOT {

namespace Internal {

namespace RDF {

namespace RDFDetail = ROOT::Detail::RDF;
namespace RDFInternal = ROOT::Internal::RDF;

class RProxyDS : public ROOT::RDF::RDataSource {
protected:
   RDFDetail::RLoopManager *fSourceLoopManager;
   ROOT::RDF::RDataSource *fDataSource;
   TTree *fTree;
   std::vector<std::unique_ptr<TTree>> fTreeViews;
   std::vector<std::unique_ptr<TTreeReader>> fReaders;
   int fNSlots = 1;
   std::vector<std::pair<ULong64_t, ULong64_t>> fSourceRanges;

   std::vector<void *> GetColumnReadersImpl(std::string_view, const std::type_info &) final { return {}; }

   bool LoadEntry(unsigned int slot, ULong64_t sourceEntry)
   {
      if (fDataSource) {
         if (!fDataSource->SetEntry(slot, sourceEntry)) {
            return false;
         }
      }

      if (fTree) {
         fReaders[slot]->SetEntry(sourceEntry);
      }

      fSourceLoopManager->RunAndCheckFilters(slot, sourceEntry);

      return true;
   }

   std::unique_ptr<TTree> makeView(TTree *tree)
   {
      auto fileNames = Internal::TreeUtils::GetFileNamesFromTree(*tree);
      auto treeNames = Internal::TreeUtils::GetTreeFullPaths(*tree);

      if (fileNames.size() != treeNames.size()) {
         throw std::runtime_error("Error in making a chain clone");
      }

      auto chain = std::make_unique<TChain>();

      for (auto i = 0u; i < fileNames.size(); i++) {
         chain->Add((fileNames[i] + "?#" + treeNames[i]).c_str());
      }

      chain->ResetBit(TObject::kMustCleanup);

      auto *elist = tree->GetEntryList();
      if (elist) {
         auto *elistCopy = new TEntryList(*elist);
         elistCopy->SetBit(TObject::kCanDelete);
         chain->SetEntryList(elistCopy);
      }

      return chain;
   }

public:
   RProxyDS(RDFDetail::RLoopManager *sourceLoopManager) : fSourceLoopManager(sourceLoopManager)
   {
      fNSlots = fSourceLoopManager->GetNSlots();

      if (fNSlots > 1) {
         ROOT::EnableThreadSafety();
      }

      fTree = sourceLoopManager->GetTree();
      fTreeViews.resize(fNSlots);
      fReaders.resize(fNSlots);

      fDataSource = fSourceLoopManager->GetDataSource();

      if (fTree) {
         TChain *chain = dynamic_cast<TChain *>(fTree);

         if (chain) {
            chain->GetEntries(); // to force the computation of nentries
            TIter next(chain->GetListOfFiles());
            TChainElement *element = 0;

            ULong64_t nEntriesSum = 0;
            while ((element = dynamic_cast<TChainElement *>(next()))) {
               ULong64_t nEntries = element->GetEntries();
               fSourceRanges.emplace_back(nEntriesSum, nEntriesSum + nEntries);
               nEntriesSum += nEntries;
            }
         } else {
            fSourceRanges.emplace_back(0, fTree->GetEntries());
         }

         for (int slot = 0; slot < fNSlots; slot++) {
            fTreeViews[slot] = makeView(fTree);
            fReaders[slot] = std::make_unique<TTreeReader>(fTreeViews[slot].get(), fTreeViews[slot]->GetEntryList());
         }
      } else if (fDataSource) {
         fDataSource->SetNSlots(fNSlots);

         // fSourceRanges is irrelevant, because there can be multiple
      } else {
         ULong64_t numberOfEntries = fSourceLoopManager->GetNEmptyEntries();

         if (fNSlots == 1) {
            fSourceRanges.emplace_back(0, numberOfEntries);
         } else {
            for (int slot = 0; slot < fNSlots; slot++) {
               double startFraq = static_cast<double>(slot) / fNSlots;
               double endFraq = static_cast<double>(slot + 1) / fNSlots;

               ULong64_t start = static_cast<ULong64_t>(startFraq * numberOfEntries);
               ULong64_t end = static_cast<ULong64_t>(endFraq * numberOfEntries);

               fSourceRanges.emplace_back(start, end);
            }
         }
      }
   }

   virtual ~RProxyDS() = default;

   virtual void SetNSlots(unsigned int nSlots) final
   {
      if (static_cast<int>(nSlots) != fNSlots) {
         throw std::runtime_error("RLoopManager: NSlots mismatch");
      }
   }

   void Initialise() final
   {
      fSourceLoopManager->Initialise();

      if (fDataSource) {
         fDataSource->Initialise();
      }

      this->InitialiseDerived();
   }
   virtual void InitialiseDerived() {}

   void InitSlot(unsigned int slot, ULong64_t firstEntry) final
   {
      fSourceLoopManager->InitNodeSlots(fReaders[slot].get(), slot);

      if (fDataSource) {
         fDataSource->InitSlot(slot, firstEntry);
      }

      this->InitSlotDerived(slot, firstEntry);
   }
   virtual void InitSlotDerived(unsigned int, ULong64_t) {}

   void FinaliseSlot(unsigned int slot) final
   {
      fSourceLoopManager->CleanUpTask(fReaders[slot].get(), slot);

      if (fDataSource) {
         fDataSource->FinaliseSlot(slot);
      }

      this->FinaliseSlotDerived(slot);
   }
   virtual void FinaliseSlotDerived(unsigned int) {}

   void Finalise() final
   {
      fSourceLoopManager->Finalise();

      this->FinaliseDerived();
   }
   virtual void FinaliseDerived() {}

   virtual std::string GetLabel() { return "RProxyDS"; }
};

} // namespace RDF

} // namespace Internal

} // namespace ROOT

#endif