package accesslog_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	. "github.com/Kong/kuma/pkg/envoy/accesslog"

	"github.com/golang/protobuf/ptypes/wrappers"

	accesslog_data "github.com/envoyproxy/go-control-plane/envoy/data/accesslog/v2"

	util_proto "github.com/Kong/kuma/pkg/util/proto"
)

var _ = Describe("ParseFormat()", func() {

	Context("valid format string", func() {

		commonProperties := &accesslog_data.AccessLogCommon{
			StartTime: util_proto.MustTimestampProto(time.Unix(1582062737, 987654321)),
		}

		httpExample := &accesslog_data.HTTPAccessLogEntry{
			CommonProperties: commonProperties,
			Request: &accesslog_data.HTTPRequestProperties{
				RequestBodyBytes: 123,
			},
			Response: &accesslog_data.HTTPResponseProperties{
				ResponseCode: &wrappers.UInt32Value{
					Value: 200,
				},
				ResponseCodeDetails: "response code details",
				ResponseBodyBytes:   456,
			},
		}

		tcpExample := &accesslog_data.TCPAccessLogEntry{
			CommonProperties: commonProperties,
			ConnectionProperties: &accesslog_data.ConnectionProperties{
				ReceivedBytes: 123,
				SentBytes:     456,
			},
		}

		type testCase struct {
			format       string
			expectedHTTP string
			expectedTCP  string
		}

		DescribeTable("should succefully parse valid format string",
			func(given testCase) {
				// when
				formatter, err := ParseFormat(given.format)
				// then
				Expect(err).ToNot(HaveOccurred())

				// when
				actual, err := formatter.FormatHttpLogEntry(httpExample)
				// then
				Expect(err).ToNot(HaveOccurred())
				// and
				Expect(actual).To(Equal(given.expectedHTTP))

				// when
				actual, err = formatter.FormatTcpLogEntry(tcpExample)
				// then
				Expect(err).ToNot(HaveOccurred())
				// and
				Expect(actual).To(Equal(given.expectedTCP))
			},
			Entry("empty string", testCase{
				format:       "",
				expectedHTTP: ``,
				expectedTCP:  ``,
			}),
			Entry("literal string", testCase{
				format:       `text without Envoy command operators`,
				expectedHTTP: `text without Envoy command operators`,
				expectedTCP:  `text without Envoy command operators`,
			}),
			Entry("%START_TIME%", testCase{
				format:       `%START_TIME%`,
				expectedHTTP: `2020-02-18T21:52:17.987Z`,
				expectedTCP:  `2020-02-18T21:52:17.987Z`,
			}),
			Entry("%START_TIME()%", testCase{
				format:       `%START_TIME%`,
				expectedHTTP: `2020-02-18T21:52:17.987Z`,
				expectedTCP:  `2020-02-18T21:52:17.987Z`,
			}),
			Entry("%START_TIME(%Y/%m/%dT%H:%M:%S%z %s)%", testCase{
				format:       `%START_TIME(%Y/%m/%dT%H:%M:%S%z %s)%`, // not supported yet
				expectedHTTP: `2020-02-18T21:52:17.987Z`,
				expectedTCP:  `2020-02-18T21:52:17.987Z`,
			}),
			Entry("%START_TIME(%s.%3f)%", testCase{
				format:       `%START_TIME(%s.%3f)%`, // not supported yet
				expectedHTTP: `2020-02-18T21:52:17.987Z`,
				expectedTCP:  `2020-02-18T21:52:17.987Z`,
			}),
			Entry("%BYTES_RECEIVED%", testCase{
				format:       `%BYTES_RECEIVED%`,
				expectedHTTP: `123`,
				expectedTCP:  `123`,
			}),
			Entry("%RESPONSE_CODE%", testCase{
				format:       `%RESPONSE_CODE%`,
				expectedHTTP: `200`,
				expectedTCP:  `0`, // TODO: how is it comparable to file access log?
			}),
			Entry("%RESPONSE_CODE_DETAILS%", testCase{
				format:       `%RESPONSE_CODE_DETAILS%`,
				expectedHTTP: `response code details`,
				expectedTCP:  `-`, // TODO: how is it comparable to file access log?
			}),
			Entry("%BYTES_SENT%", testCase{
				format:       `%BYTES_SENT%`,
				expectedHTTP: `456`,
				expectedTCP:  `456`,
			}),
		)
	})

	Context("invalid format string", func() {

		type testCase struct {
			format      string
			expectedErr string
		}

		DescribeTable("should reject an invalid format string",
			func(given testCase) {
				// when
				formatter, err := ParseFormat(given.format)
				// then
				Expect(formatter).To(BeNil())
				// and
				Expect(err).To(HaveOccurred())
				// and
				Expect(err.Error()).To(Equal(given.expectedErr))
			},
			Entry("unbalanced %", testCase{
				format:      `text with % character`,
				expectedErr: `format string is not valid: expected a command operator at position 10`,
			}),
			Entry("%START_TIME(%", testCase{
				format:      `%START_TIME(%`,
				expectedErr: `format string is not valid: expected a command operator at position 0`,
			}),
			Entry("%BYTES_RECEIVED()%", testCase{
				format:      `%BYTES_RECEIVED()%`,
				expectedErr: `format string is not valid: command "%BYTES_RECEIVED%" doesn't support arguments or max length constraint, instead got "%BYTES_RECEIVED()%"`,
			}),
		)
	})
})
