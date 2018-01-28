middleware-match 阿里中间件比赛初复赛代码（包含第二届、第三届）
---
* middleware-match2-1 第二届阿里中间件性能挑战赛初赛代码（初赛第35名，前100进入复赛）
* middleware-match2-2 第二届阿里中间件性能挑战赛复赛代码（复赛第38名，共1943支队伍报名）
* middleware-match3-1 第三届阿里中间件性能挑战赛初赛代码（初赛第15名，前100进入复赛）
* middleware-match3-2 第三届阿里中间件性能挑战赛复赛代码（复赛第8名，共1959支队伍报名）


# middleware-match2-1
## 1背景
该项目是<a href="https://tianchi.aliyun.com/competition/information.htm?spm=5176.100067.5678.2.4c5fd3bZTtceN&raceId=231533">第二届阿里中间件性能挑战赛</a>初赛代码。

## 2解题思路
只是一个简单实现，按消息类型划分了任务

# middleware-match2-2
1. 背景
该项目是<a href="https://tianchi.aliyun.com/competition/information.htm?spm=5176.100067.5678.2.4c5fd3bZTtceN&raceId=231533">第二届阿里中间件性能挑战赛</a>复赛代码。

2. 解题思路
* 文件读后不转存，只是记录索引
* 使用hash划分文件索引
* 查询使用缓存

3. 反思
成绩不佳，没有很好利用内存。


# middleware-match3-1
<a href="https://tianchi.aliyun.com/programming/information.htm?spm=5176.100067.5678.2.26939b3aW0K6r7&raceId=231600">第三届阿里中间件挑战赛</a> 的初赛代码，该版本不是最优版本，还需要替换Snappy压缩算法。
主要优化：1）数据压缩进行 IO 优化；2）按线程粒度存文件进行无锁化处理。

# middleware-match3-2
<a href="https://tianchi.aliyun.com/programming/information.htm?spm=5176.100067.5678.2.26939b3aW0K6r7&raceId=231600">第三届阿里中间件挑战赛</a> 的复赛代码。
主要优化：1）并行化处理（流水线）；2）IO 优化（读取文件大小调参、网路传输自定义格式）；3）预测技术（局部相似性原理）；4）map 优化（分桶减少 hash 冲突）5）减少 GC 和数据拷贝。
