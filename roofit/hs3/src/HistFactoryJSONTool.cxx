#include <RooFitHS3/RooJSONFactoryWSTool.h>
#include <RooFitHS3/HistFactoryJSONTool.h>

#include "RooStats/HistFactory/Measurement.h"
#include "RooStats/HistFactory/Channel.h"
#include "RooStats/HistFactory/Sample.h"

#include "JSONInterface.h"

#ifdef ROOFIT_HS3_WITH_RYML
#include "RYMLParser.h"
typedef TRYMLTree tree_t;
#else
#include "JSONParser.h"
typedef TJSONTree tree_t;
#endif

using RooFit::Detail::JSONNode;

RooStats::HistFactory::JSONTool::JSONTool(RooStats::HistFactory::Measurement *m) : _measurement(m){};

void RooStats::HistFactory::JSONTool::Export(const RooStats::HistFactory::Sample &sample, JSONNode &s) const
{
   std::vector<std::string> obsnames;
   obsnames.push_back("obs_x_" + sample.GetChannelName());
   obsnames.push_back("obs_y_" + sample.GetChannelName());
   obsnames.push_back("obs_z_" + sample.GetChannelName());

   s.set_map();
   s["type"] << "histogram";

   if (sample.GetOverallSysList().size() > 0) {
      auto &overallSys = s["overallSystematics"];
      overallSys.set_map();
      for (const auto &sys : sample.GetOverallSysList()) {
         auto &node = overallSys[sys.GetName()];
         node.set_map();
         node["low"] << sys.GetLow();
         node["high"] << sys.GetHigh();
      }
   }

   if (sample.GetNormFactorList().size() > 0) {
      auto &normFactors = s["normFactors"];
      normFactors.set_seq();
      for (auto &sys : sample.GetNormFactorList()) {
         normFactors.append_child() << sys.GetName();
      }
   }

   if (sample.GetHistoSysList().size() > 0) {
      auto &histoSys = s["histogramSystematics"];
      histoSys.set_map();
      for (size_t i = 0; i < sample.GetHistoSysList().size(); ++i) {
         auto &sys = sample.GetHistoSysList()[i];
         auto &node = histoSys[sys.GetName()];
         node.set_map();
         auto &dataLow = node["dataLow"];
         auto &dataHigh = node["dataHigh"];
         RooJSONFactoryWSTool::exportHistogram(*(sys.GetHistoLow()), dataLow, obsnames);
         RooJSONFactoryWSTool::exportHistogram(*(sys.GetHistoHigh()), dataHigh, obsnames);
      }
   }

   // std::vector< RooStats::HistFactory::ShapeSys >    fShapeSysList;
   // std::vector< RooStats::HistFactory::ShapeFactor > fShapeFactorList;

   auto &tags = s["dict"];
   tags.set_map();
   tags["normalizeByTheory"] << sample.GetNormalizeByTheory();

   s["statError"] << sample.GetStatError().GetActivate();

   auto &data = s["data"];
   RooJSONFactoryWSTool::exportHistogram(*sample.GetHisto(), data, obsnames,
                                         sample.GetStatError().GetActivate() && sample.GetStatError().GetUseHisto()
                                            ? sample.GetStatError().GetErrorHist()
                                            : nullptr);
}

void RooStats::HistFactory::JSONTool::Export(const RooStats::HistFactory::Channel &c, JSONNode &ch) const
{
   ch.set_map();
   ch["type"] << "histfactory";

   auto &staterr = ch["statError"];
   staterr.set_map();
   staterr["relThreshold"] << c.GetStatErrorConfig().GetRelErrorThreshold();
   staterr["constraint"] << RooStats::HistFactory::Constraint::Name(c.GetStatErrorConfig().GetConstraintType());

   auto &samples = ch["samples"];
   samples.set_map();
   for (const auto &s : c.GetSamples()) {
      auto &sample = samples[s.GetName()];
      this->Export(s, sample);
      auto &ns = sample["namespaces"];
      ns.set_seq();
      ns.append_child() << c.GetName();
   }
}

void RooStats::HistFactory::JSONTool::Export(JSONNode &n) const
{
   for (const auto &ch : this->_measurement->GetChannels()) {
      if (!ch.CheckHistograms())
         throw std::runtime_error("unable to export histograms, please call CollectHistograms first");
   }

   auto &pdflist = n["pdfs"];
   pdflist.set_map();

   // collect information
   std::map<std::string, RooStats::HistFactory::Constraint::Type> constraints;
   std::map<std::string, NormFactor> normfactors;
   for (const auto &ch : this->_measurement->GetChannels()) {
      for (const auto &s : ch.GetSamples()) {
         for (const auto &sys : s.GetOverallSysList()) {
            constraints[sys.GetName()] = RooStats::HistFactory::Constraint::Gaussian;
         }
         for (const auto &sys : s.GetHistoSysList()) {
            constraints[sys.GetName()] = RooStats::HistFactory::Constraint::Gaussian;
         }
         for (const auto &sys : s.GetShapeSysList()) {
            constraints[sys.GetName()] = sys.GetConstraintType();
         }
         for (const auto &norm : s.GetNormFactorList()) {
            normfactors[norm.GetName()] = norm;
         }
      }
   }

   // preprocess functions
   if (this->_measurement->GetFunctionObjects().size() > 0) {
      auto &funclist = n["functions"];
      funclist.set_map();
      for (const auto &func : this->_measurement->GetFunctionObjects()) {
         auto &f = funclist[func.GetName()];
         f.set_map();
         f["name"] << func.GetName();
         f["expression"] << func.GetExpression();
         f["dependents"] << func.GetDependents();
         f["command"] << func.GetCommand();
      }
   }

   // the simpdf
   auto &sim = pdflist[this->_measurement->GetName()];
   sim.set_map();
   sim["type"] << "simultaneous";
   auto &simdict = sim["dict"];
   simdict.set_map();
   simdict["InterpolationScheme"] << this->_measurement->GetInterpolationScheme();
   auto &simtags = sim["tags"];
   simtags.set_seq();
   simtags.append_child() << "toplevel";
   auto &ch = sim["channels"];
   ch.set_map();
   for (const auto &c : this->_measurement->GetChannels()) {
      auto &thisch = ch[c.GetName()];
      this->Export(c, thisch);
   }

   // the variables
   auto &varlist = n["variables"];
   varlist.set_map();
   for (const auto &c : this->_measurement->GetChannels()) {
      for (const auto &s : c.GetSamples()) {
         for (const auto &norm : s.GetNormFactorList()) {
            if (!varlist.has_child(norm.GetName())) {
               auto &v = varlist[norm.GetName()];
               v.set_map();
               v["value"] << norm.GetVal();
               v["min"] << norm.GetLow();
               v["max"] << norm.GetHigh();
               if (norm.GetConst()) {
                  v["const"] << true;
               }
            }
         }
         for (const auto &sys : s.GetOverallSysList()) {
            std::string parname("alpha_");
            parname += sys.GetName();
            if (!varlist.has_child(parname)) {
               auto &v = varlist[parname];
               v.set_map();
               v["value"] << 0.;
               v["min"] << -5.;
               v["max"] << 5.;
            }
         }
      }
   }
   for (const auto &sys : this->_measurement->GetConstantParams()) {
      std::string parname = "alpha_" + sys;
      if (!varlist.has_child(parname)) {
         auto &v = varlist[parname];
         v.set_map();
      }
      varlist[parname]["const"] << true;
   }

   for (const auto &poi : this->_measurement->GetPOIList()) {
      if (!varlist[poi].has_child("tags")) {
         auto &tags = varlist[poi]["tags"];
         tags.set_seq();
      }
      varlist[poi]["tags"].append_child() << "poi";
   }

   // the data
   auto &datalist = n["data"];
   datalist.set_map();
   auto &obsdata = datalist["obsData"];
   obsdata.set_map();
   obsdata["index"] << "channelCat";
   for (const auto &c : this->_measurement->GetChannels()) {
      std::vector<std::string> obsnames;
      obsnames.push_back("obs_x_" + c.GetName());
      obsnames.push_back("obs_y_" + c.GetName());
      obsnames.push_back("obs_z_" + c.GetName());

      auto &chdata = obsdata[c.GetName()];
      RooJSONFactoryWSTool::exportHistogram(*c.GetData().GetHisto(), chdata, obsnames);
   }
}

void RooStats::HistFactory::JSONTool::PrintJSON(std::ostream &os)
{
   tree_t p;
   auto &n = p.rootnode();
   n.set_map();
   this->Export(n);
   n.writeJSON(os);
}
void RooStats::HistFactory::JSONTool::PrintJSON(std::string const &filename)
{
   std::ofstream out(filename);
   this->PrintJSON(out);
}

#ifdef ROOFIT_HS3_WITH_RYML
void RooStats::HistFactory::JSONTool::PrintYAML(std::ostream &os)
{
   TRYMLTree p;
   auto &n = p.rootnode();
   n.set_map();
   this->Export(n);
   n.writeYML(os);
}
#else
void RooStats::HistFactory::JSONTool::PrintYAML(std::ostream & /*os*/)
{
   std::cerr << "YAML export only support with rapidyaml!" << std::endl;
}
#endif

void RooStats::HistFactory::JSONTool::PrintYAML(std::string const &filename)
{
   std::ofstream out(filename);
   this->PrintYAML(out);
}
