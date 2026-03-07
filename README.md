im building something personal, this is the part I wish to expose to others.
I control my concurrency but I wanted a blazingly fast write oriented embeddable storage engine.
I also wrote the rest of the tcp server in C# so needed it in C#. Faster is .... fast, sqlite is not as fast
I used this as a workaround to use faster in hotpath and amortize cost of writing to Sqlite for maintaing a sparse index.
