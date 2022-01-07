// Author:

/*************************************************************************
 * Copyright (C) 1995-2018, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RPROXYDS
#define ROOT_RPROXYDS

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

   std::vector<void *> GetColumnReadersImpl(std::string_view, const std::type_info &) final { return {}; }

   bool LoadEntry(unsigned int slot, ULong64_t sourceEntry)
   {
      if (fDataSource) {
         if (!fDataSource->SetEntry(slot, sourceEntry)) {
            return false;
         }
      }

      fSourceLoopManager->RunAndCheckFilters(slot, sourceEntry);

      return true;
   }

public:
   RProxyDS(RDFDetail::RLoopManager *sourceLoopManager) : fSourceLoopManager(sourceLoopManager)
   {
      fDataSource = fSourceLoopManager->GetDataSource();

      if (fDataSource) {
         fDataSource->SetNSlots(fSourceLoopManager->GetNSlots());
      }
   }

   virtual ~RProxyDS() = default;

   virtual void SetNSlots(unsigned int nSlots) final
   {
      if (nSlots != fSourceLoopManager->GetNSlots()) {
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
      fSourceLoopManager->InitNodeSlots(nullptr, slot);

      if (fDataSource) {
         fDataSource->InitSlot(slot, firstEntry);
      }

      this->InitSlotDerived(slot, firstEntry);
   }
   virtual void InitSlotDerived(unsigned int, ULong64_t) {}

   void FinaliseSlot(unsigned int slot) final
   {
      fSourceLoopManager->CleanUpTask(nullptr, slot);

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