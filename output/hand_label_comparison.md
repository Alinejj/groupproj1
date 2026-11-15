# Hand labels vs. model predictions (2.3b)

- Messages hand-labeled: 49
- Fit none of the four classes (None of these): 13 (26.5%)
- Fit one of the four classes: 36
- **Accuracy on messages that fit one of the four classes: 27/36 = 75.0%**

## Confusion matrix (rows = hand label, columns = model prediction)

| Hand \ Predicted | World | Sports | Business | Sci/Tech |
|---|---:|---:|---:|---:|
| World | 2 | 0 | 2 | 1 |
| Sports | 0 | 0 | 0 | 0 |
| Business | 1 | 2 | 5 | 1 |
| Sci/Tech | 1 | 1 | 0 | 20 |
| None of these | 0 | 2 | 5 | 6 |

## Accuracy by hand label

| Hand label | Messages | Model correct |
|---|---:|---:|
| World | 5 | 2 (40%) |
| Business | 9 | 5 (56%) |
| Sci/Tech | 22 | 20 (91%) |

## Where the None-of-these messages went

- Sci/Tech: 6
- Business: 5
- Sports: 2

## Messages the model got wrong (fit a class but predicted another)

| Message | Predicted (probability) | Hand label | Notes |
|---|---|---|---|
| Huang is the classic example of an entrepreneur who grew up and came of age at the end of the Cold War, at the height of globalization and the Pax Americana:... | Business (0.443) | World | Globalization, Cold War, and national interests; could be Business |
| When Xi visited, Trump offered to sell him US made weapons. What's the chance that these GPUs got to China without Trump getting a cut somehow? Pretty much z... | Business (0.7599) | World | politics; also mentions GPUs |
| Yeah I'm not actually a fan of the company itself, it seems to have disingenuous and self-serving morals. It’s “Anthropic is against violence and we will not... | Sports (0.988) | Business | company ethics/military deal; could be World |
| Yes or indeed supplying cards to the Chinese market with extras... | World (0.719) | Business | Graphics card supply to Chinese market; could be Sci/Tech |
| He's had a couple of Anthropic sponsored videos as well so Claude seems like a safe assumption here too. | Sports (0.5691) | Business | Anthropic sponsorships; could be Sci/Tech |
| > Farmed trees are often promoted as being a great fuel, but they do very, very little for enhancing biodiversity. Wind turbines and hydroelectric dams don't... | World (0.9793) | Sci/Tech | biomass energy and CO2 emissions, could be World |
| About 2 years ago, claude only had 200k(250k?) context; Even with clear instructions, the agents were not that disciplined; they usually suffered from attent... | Sports (0.9892) | Sci/Tech | AI coding agents |
| it's funny because it convinces me that export controls don't work so why do them | Sci/Tech (0.8764) | World | export controls; could be Business |
| But like is Huang supposed to personally vet every customer, seem preposterous. The only real culpability angle is if Huang influenced the process to not vet... | Sci/Tech (0.971) | Business | Nvidia/Huang customer vetting, could be World |
