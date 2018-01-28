middleware-match 
---
该项目是本人参加过的阿里中间件比赛系列代码，包括：第二届初复赛、第三届初复赛。
* middleware-match2-1 第二届初赛代码（初赛第35名，前100进入复赛）
* middleware-match2-2 第二届复赛代码（复赛第38名，共1943支队伍报名）
* middleware-match3-1 第三届初赛代码（初赛第15名，前100进入复赛）
* middleware-match3-2 第三届复赛代码（复赛第8名，共1959支队伍报名）


# middleware-match2-1
1. 背景<br>
该项目是<a href="https://tianchi.aliyun.com/competition/information.htm?spm=5176.100067.5678.2.4c5fd3bZTtceN&raceId=231533">第二届阿里中间件性能挑战赛</a>初赛代码。

2. 解题思路<br>
只是一个简单实现，按消息类型划分了任务

# middleware-match2-2
1. 背景<br>
该项目是<a href="https://tianchi.aliyun.com/competition/information.htm?spm=5176.100067.5678.2.4c5fd3bZTtceN&raceId=231533">第二届阿里中间件性能挑战赛</a>复赛代码。

2. 解题思路<br>
* 文件读后不转存，只是记录索引
* 使用hash划分文件索引
* 查询使用缓存

3. 反思<br>
成绩不佳，没有很好利用内存。


# middleware-match3-1
<a href="https://tianchi.aliyun.com/programming/information.htm?spm=5176.100067.5678.2.26939b3aW0K6r7&raceId=231600">第三届阿里中间件挑战赛</a> 的初赛代码，该版本不是最优版本，还需要替换Snappy压缩算法。
主要优化：
* 数据压缩进行 IO 优化
* 按线程粒度存文件进行无锁化处理。

# middleware-match3-2
[第三届阿里中间件挑战赛复赛](https://tianchi.aliyun.com/programming/information.htm?spm=5176.100067.5678.2.26939b3aW0K6r7&raceId=231600)代码。主要优化如下所示：
* 并行化处理（流水线）；
* IO 优化（读取文件大小调参、网路传输自定义格式）；
* 预测技术（局部相似性原理）；
* map 优化（分桶减少 hash 冲突）
* 减少 GC 和数据拷贝。

一些资源:<br>
* [答辩PPT](https://mindawei.github.io/about/docs/ppt/middleware.pdf)
* [总决赛优胜奖队伍_也许放弃才能靠近你_比赛攻略](https://tianchi.aliyun.com/forum/new_articleDetail.html?spm=5176.11165354.0.0.7f90e058s0HB6E&from=user&raceId=&postsId=2018)

# 总结
有些思路可能看了之后也觉得没什么，但是要在有限的时间内给出正确的实现就要求具备扎实的基本功。经历过，才知道要更加努力。不要妄自菲薄，也不要恃才放旷，一点点积累，快乐就好。
