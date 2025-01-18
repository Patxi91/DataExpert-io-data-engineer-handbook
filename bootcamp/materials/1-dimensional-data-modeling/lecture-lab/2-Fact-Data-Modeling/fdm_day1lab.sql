------------------------------------------------
-----      LAB 1: Fact Data Modeling       -----
------------------------------------------------
/*
Fact Modeling: Who, What, Where, When and How:
    - Who: IDs (Car ID, User ID, OS ID, Phone ID)
    - Where: Geographical(country), Where in the app (Homepage, Profile page): IDs, /where or /profile, etc.
    - How: Similar to where/how --> "He used an iphone to make this click" --> (How: iphone, Where: Homepage)
    - What: The atomic event that happened, a notification was "generated", "sent", "clicked", "delivered"
    - When: "event_timestamp", "event_date" (Client-side logging, ideally UTC)
Options when working with high-volume data:
    - Sampling: metric-driven use-cases
    - Bucketing:
        - Fact data by an important dimension (usually user)
        - Bucket joins > Shuffle joins
        - Sorted-merge-Bucket (SMB) joins can do joins without Shuffle at all!
*/

---------
--  X  --
---------
