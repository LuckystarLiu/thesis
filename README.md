# thesis
Collaborative Filtering - Restaurant Recommender System (Yelp Dataset Challenge)


The goal of this project is to implement a recommender system, using a method for recommending items to users based on expert opinions. These expert opinions are weighted according to their similarity to the user. Instead of applying the nearest neighbour algorithm to the user-rating data directly, user-expert ratings calculated from an independent expert dataset is used. Since nearest-neighbour CF suffers from data sparsity, cold-start and noise in user data, this method attempts to address some of these shortcomings of standard CF. Specifically:

Q.1. Study how a standard nearest neighbour model compares with an expert rating
based model for a CF based recommender system, and whether these experts (professional raters) are capable of generating good recommendations for a larger disjoint
subset of users.

Q.2. Can this expert-CF model overcome the common problems of a standard CF
model namely - cold start, sparsity and noise in user data.

The aim of this project is to build a restaurant recommender system from Yelp dataset. Using above mentioned modified form of CF technique from [3], we aim to predict Yelp user ratings for restaurants based on their previous ratings and another set of ratings from professional raters to answer the following question.

”Can a small and independent subset of users predict the travel preferences for a larger group of users? Specifically, it proposes to investigate if a CF technique, using an independent and uncorrelated expert rating dataset, outperform the standard neighbourhood-based CF technique?

