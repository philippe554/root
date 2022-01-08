// Author:

/*************************************************************************
 * Copyright (C) 1995-2022, Rene Brun and Fons Rademakers.               *
 * All rights reserved.                                                  *
 *                                                                       *
 * For the licensing terms see $ROOTSYS/LICENSE.                         *
 * For the list of contributors see $ROOTSYS/README/CREDITS.             *
 *************************************************************************/

#ifndef ROOT_RDF_RDEFINEPERSISTENT
#define ROOT_RDF_RDEFINEPERSISTENT

#include "ROOT/RDF/ColumnReaderUtils.hxx"
#include "ROOT/RDF/RActionBase.hxx"
#include "ROOT/RDF/RColumnReaderBase.hxx"
#include "ROOT/RDF/RDefineBase.hxx"
#include "ROOT/RDF/RLoopManager.hxx"
#include "ROOT/RDF/Utils.hxx"
#include "ROOT/RStringView.hxx"
#include "ROOT/TypeTraits.hxx"
#include "RtypesCore.h"

#include <array>
#include <deque>
#include <type_traits>
#include <utility> // std::index_sequence
#include <vector>

class TTreeReader;

namespace ROOT {
namespace Detail {
namespace RDF {

using namespace ROOT::TypeTraits;

template <typename F>
class R__CLING_PTRCHECK(off) RDefinePersistent final : public RDefineBase {
   using FunParamTypes_t = typename CallableTraits<F>::arg_types;
   using PersistentParamType_t = TakeFirstParameter_t<FunParamTypes_t>;
   using PersistentParamTypePerSlot_t =
      std::conditional_t<std::is_same<PersistentParamType_t, bool>::value, std::deque<PersistentParamType_t>,
                         std::vector<PersistentParamType_t>>;
   using ColumnTypes_t = RemoveFirstParameter_t<FunParamTypes_t>;
   using TypeInd_t = std::make_index_sequence<ColumnTypes_t::list_size>;

   F fExpression;
   PersistentParamTypePerSlot_t fPersistantState;

   /// Column readers per slot and per input column
   std::vector<std::array<std::unique_ptr<RColumnReaderBase>, ColumnTypes_t::list_size>> fValues;

   std::unique_ptr<RDFInternal::RActionBase> fEvalAction;

   template <typename... ColTypes, std::size_t... S>
   void UpdateHelper(unsigned int slot, Long64_t entry, TypeList<ColTypes...>, std::index_sequence<S...>)
   {
      fExpression(fPersistantState[slot * RDFInternal::CacheLineStep<PersistentParamType_t>()],
                  fValues[slot][S]->template Get<ColTypes>(entry)...);

      // silence "unused parameter" warnings in gcc
      (void)slot;
      (void)entry;
   }

public:
   RDefinePersistent(std::string_view name, std::string_view type, F expression,
                     const ROOT::RDF::ColumnNames_t &columns, const RDFInternal::RColumnRegister &colRegister,
                     RLoopManager &lm)
      : RDefineBase(name, type, colRegister, lm, columns), fExpression(std::move(expression)),
        fPersistantState(lm.GetNSlots() * RDFInternal::CacheLineStep<PersistentParamType_t>()), fValues(lm.GetNSlots())
   {
   }

   RDefinePersistent(const RDefinePersistent &) = delete;
   RDefinePersistent &operator=(const RDefinePersistent &) = delete;

   void InitSlot(TTreeReader *r, unsigned int slot) final
   {
      RDFInternal::RColumnReadersInfo info{fColumnNames, fColRegister, fIsDefine.data(), fLoopManager->GetDSValuePtrs(),
                                           fLoopManager->GetDataSource()};
      fValues[slot] = RDFInternal::MakeColumnReaders(slot, r, ColumnTypes_t{}, info);
      fLastCheckedEntry[slot * RDFInternal::CacheLineStep<Long64_t>()] = -1;
      fPersistantState[slot * RDFInternal::CacheLineStep<PersistentParamType_t>()] = PersistentParamType_t();
   }

   /// Return the (type-erased) address of the Define'd value for the given processing slot.
   void *GetValuePtr(unsigned int slot) final
   {
      return static_cast<void *>(&fPersistantState[slot * RDFInternal::CacheLineStep<PersistentParamType_t>()]);
   }

   /// Update the value at the address returned by GetValuePtr with the content corresponding to the given entry
   void Update(unsigned int slot, Long64_t entry) final
   {
      if (entry < fLastCheckedEntry[slot * RDFInternal::CacheLineStep<Long64_t>()]) {
         throw std::runtime_error("RDefinePersistent can't iterate backwards");
      }

      if (entry != fLastCheckedEntry[slot * RDFInternal::CacheLineStep<Long64_t>()]) {
         // evaluate this define expression, cache the result
         UpdateHelper(slot, entry, ColumnTypes_t{}, TypeInd_t{});
         fLastCheckedEntry[slot * RDFInternal::CacheLineStep<Long64_t>()] = entry;
      }
   }

   void Update(unsigned int /*slot*/, const ROOT::RDF::RSampleInfo & /*id*/) final {}

   const std::type_info &GetTypeId() const { return typeid(PersistentParamType_t); }

   /// Clean-up operations to be performed at the end of a task.
   void FinaliseSlot(unsigned int slot) final
   {
      for (auto &v : fValues[slot])
         v.reset();
   }

   void GiveEvalAction(std::unique_ptr<RDFInternal::RActionBase> evalAction) { fEvalAction = std::move(evalAction); }
};

} // namespace RDF
} // namespace Detail
} // namespace ROOT

#endif // ROOT_RDF_RDEFINE
