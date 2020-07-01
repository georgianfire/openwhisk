def isPrime(n):
    for i in range(2, n):
        if n % i == 0:
            return False
    return True

def main(args):
    n = args.get("n", "3000")
    n = int(n)
    primes = []
    for i in range(2, n):
        if isPrime(i):
            primes.append(i)
    return {"total": len(primes)}
