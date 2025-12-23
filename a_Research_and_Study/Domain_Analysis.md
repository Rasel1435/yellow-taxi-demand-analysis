## 🧠 NYC Taxi Demand: Domain Study & Problem Definition
**1. The Core Objective**

The goal is to solve the **Supply-Demand Imbalance.** In New York City, taxi drivers often lose money by idling in low-demand areas, while passengers in high-demand areas face long wait times. By predicting demand, we can suggest where "supply" (drivers) should be positioned 1–2 hours in advance.

**2. Influencing Factors (The "Signals")**

Through EDA, we identified three primary drivers of demand:
- **Temporal Seasonality:** Demand is highly cyclical. We see "Rush Hour" peaks on weekdays and "Nightlife" peaks in the Meatpacking District/Lower East Side on weekends.
- **Weather Events:** Precipitation significantly increases taxi demand as people avoid walking or taking the subway.
- **Major Events:** Broadway shows letting out or sports events at Madison Square Garden create massive, localized demand spikes.

**3. Feature Engineering LogicLags**
- **(T-1 to T-24):** Crucial for capturing "Momentum." If demand was high an hour ago, it is likely to stay high for the next 30 minutes.
- **Rolling Windows (7-Day Mean):** This captures the "Baseline." It helps the model understand what a "Normal Monday" looks like compared to a "Holiday Monday."

**4. Evaluation Metrics for the Business**

While we use $R^2$ and $MAPE$ for training, the business cares about:
- **Under-prediction:** Leads to lost revenue (passengers can't find a car).
- **Over-prediction:** Leads to wasted fuel and driver frustration (too many cars in one area).
