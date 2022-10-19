# Restaurant Platform (HM-DianPing)

It is a project of an open class offered by Itheima. The template of the project is provided by the open class resource, and I focused on the service of login, details query, flash sale, blog post, like, follow, sign and restaurant nearby. I also implemented necessary methods to maintain the robustness of the system.

** **

## Login/register by message authentication code
The system will generate 6 digits verification code, and the code will stored in Redis for 2 minutes. During this time, if the user send the right verification code from frontend, the system will generate a token as the id and store in redis for 30 minutes. There is also an interceptor to make sure the active users can always in login status.

** **

## Details query
It is the service when the user want to see the details of a restaurant. The system will save the details to Redis for 30 minutes for high effiency. And I implemented three different functions to handle different cache problems, including cache pass through and cache breakdown. And the update of shop in database will delete the shop details in cache for consistency.

** **
## Flash sale
It requires the system can handle high concurrency situations. The distributed lock is implemented by Redis, which can ensure the one user can only buy once and no oversold. The system finally use the Redisson libaray to get more high quality lock. Lua script for Redis is used to ensure the atomicity of multi-steps Redis operation. It also used message queue based on Redis stream to improve the response efficiency.

** **
## Blog post & Like
The blog details from frontend will store into MySQL database. And like information will store in both MySQL and Redis. The top n first like user of the blog will be shown on page. It is implemented by Redis SortedSet.

** **
## Follow
Follow service is implemented by Redis Set. And the common followers of two users will be shown in homepage of the user.

** **
## Sign
Sign service is implemented by Redis BitMap. It also provided the interface to compute the days of continuous sign.

** **
## Shops nearby
Shops nearby service is implemented by Redis Geo. The system will get the shops near the user within 5 km.

** **
## UV analysis
UV analysis is implemented by Redis HypeLogLog.