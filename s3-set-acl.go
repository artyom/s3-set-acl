// Command s3-set-acl scans s3 bucket and sets 'private' canned ACL on every
// object.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/artyom/autoflags"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/errgroup"
)

func main() {
	args := struct {
		Bucket string `flag:"bucket,bucket name to scan"`
		State  string `flag:"state,file to periodically save last processed key, used to continue operations"`
	}{
		State: "/tmp/fixacl-lastkey.txt",
	}
	autoflags.Parse(&args)
	if err := run(args.Bucket, args.State); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(bucket, lastKeyFile string) error {
	if bucket == "" || lastKeyFile == "" {
		return fmt.Errorf("both -bucket and -state should be set")
	}
	sess, err := session.NewSession()
	if err != nil {
		return err
	}
	svc := s3.New(sess)
	work := make(chan string)
	group, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < 10; i++ {
		group.Go(func() error {
			input := &s3.PutObjectAclInput{
				ACL:    aws.String(s3.ObjectCannedACLPrivate),
				Bucket: &bucket,
			}
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case s, ok := <-work:
					if !ok {
						return nil
					}
					input.Key = &s
					if _, err := svc.PutObjectAclWithContext(ctx, input); err != nil {
						return err
					}
				}
			}
		})
	}
	var nKeys int
	defer func() { log.Println("keys processed:", nKeys) }()
	start := time.Now()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	walkFn := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}
			s, err := url.QueryUnescape(*obj.Key)
			if err != nil {
				log.Println(*obj.Key, err)
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case work <- s:
				nKeys++
			}
			select {
			default:
			case now := <-ticker.C:
				log.Printf("processed %d keys (%.0f keys/second)", nKeys,
					float64(nKeys)/now.Sub(start).Seconds())
				_ = ioutil.WriteFile(lastKeyFile, []byte(s), 0666)
			}
		}
		return true
	}
	group.Go(func() error {
		defer close(work)
		input := &s3.ListObjectsV2Input{
			Bucket:       &bucket,
			EncodingType: aws.String(s3.EncodingTypeUrl),
		}
		if b, err := ioutil.ReadFile(lastKeyFile); err == nil {
			input.StartAfter = aws.String(string(bytes.TrimSpace(b)))
		}
		return svc.ListObjectsV2PagesWithContext(ctx, input, walkFn)
	})
	return group.Wait()
}
