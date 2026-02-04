# marginal-emissions-germany
This project contains a Python pipeline to compute marginal emission factors for the German electricity market. It processes high-resolution historical open source market data from official sources to model market dynamics beyond the merit-order principle and assess the environmental impact of heatpumps, based on their marginal emissions.


---
## Appendix
### [Important links]
#### ENTSOe
- [ENTSO-E API Documentation](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#intro)
- [API Parameter Guide](https://transparencyplatform.zendesk.com/hc/en-us/articles/15692855254548-Sitemap-for-Restful-API-Integration)
- [EIC Manual & Codes](https://www.entsoe.eu/data/energy-identification-codes-eic/)
- [Transparency Platform Guide](https://transparencyplatform.zendesk.com/hc/en-us/categories/13771885458964-Guides) <!-- Data consumers: MoP Ref2 and Ref19 recommended -->
- [Transparency Platform Knowledge Base](https://transparencyplatform.zendesk.com/hc/en-us/categories/12818231533716-Knowledge-base)
- [Manual of Procedures](https://www.entsoe.eu/data/transparency-platform/mop/)
- [Manual of Procedures v3.5 Download with Material](https://eepublicdownloads.blob.core.windows.net/public-cdn-container/clean-documents/mc-documents/transparency-platform/MOP/MoP_v3r5_final.zip)
  - File Detailed Data Description: MoP Ref2 DDD v3r5
  - File Manual of Procedures: MoP v3r5
- [Data Description Actual Generation per Generation Unit](https://transparencyplatform.zendesk.com/hc/en-us/articles/16648326220564-Actual-Generation-per-Generation-Unit-16-1-A)
- [Data Description Actual Generation per Production Type](https://transparencyplatform.zendesk.com/hc/en-us/articles/16648290299284-Actual-Generation-per-Production-Type-16-1-B-C)

### SMARD
- [SMARD API Documentation](https://smard.api.bund.dev/)

### Agora

### MSDR
- [MarkovRegression Model Documentation](https://www.statsmodels.org/stable/generated/statsmodels.tsa.regime_switching.markov_regression.MarkovRegression.html)
- 