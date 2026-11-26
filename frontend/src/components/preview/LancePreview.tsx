import { useState } from 'react'
import { keepPreviousData, useQuery } from '@tanstack/react-query'
import { Link, Navigate } from 'react-router-dom'

import { apiClient, ApiError } from '@/api/client'
import { encodeKey } from '@/lib/path'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { DataTable, type DataTableColumn } from './DataTable'
import { PageControls } from './PageControls'
import type { PreviewerProps } from './types'

interface Page {
  version?: number
  file_format?: string
  total: number
  offset: number
  columns: DataTableColumn[]
  rows: Record<string, unknown>[]
}

const pageSize = 20

export function LancePreview(props: PreviewerProps) {
  return <DatasetPage key={JSON.stringify([props.storage, props.fileKey])} {...props} />
}

function DatasetPage({ storage, fileKey }: PreviewerProps) {
  const [cursor, setCursor] = useState<{ page: number; version?: number }>({ page: 0 })
  const physical = !fileKey.endsWith('/')
  const directory = `/s/${encodeURIComponent(storage ?? '')}/${encodeKey(fileKey)}/`
  const query = useQuery({
    queryKey: ['lance', storage, fileKey, cursor],
    queryFn: async ({ signal }) => {
      const { data } = await apiClient.get<Page>(`/api/lance/${encodeKey(fileKey)}`, {
        params: { storage, physical: physical || undefined, offset: cursor.page * pageSize, version: cursor.version },
        signal,
      })
      return data
    },
    placeholderData: keepPreviousData,
    retry: false,
    refetchOnWindowFocus: false,
  })
  if (query.isPending) {
    return (
      <div className="w-full space-y-2 p-3">
        {[0, 1, 2, 3].map(i => <Skeleton key={i} className="h-8 w-full" />)}
      </div>
    )
  }
  if (!physical && query.isError && query.error instanceof ApiError && query.error.isNotFound && !cursor.version) {
    return <Navigate replace to={directory} />
  }
  const browse = !physical && (
    <Button asChild variant="outline" size="sm">
      <Link to={directory}>Browse directory</Link>
    </Button>
  )
  if (query.isError) {
    return (
      <Alert variant="destructive">
        <AlertDescription>{query.error.message}</AlertDescription>
        <Button onClick={() => query.refetch()}>Retry</Button>
        {browse}
      </Alert>
    )
  }
  const data = query.data
  const jump = (page: number) => setCursor({ page, version: data.version })
  return (
    <div className="flex min-h-0 w-full flex-col gap-2 overflow-auto p-2">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <span>{physical
          ? `Lance file · format ${data.file_format} · ${data.total.toLocaleString()} physical rows`
          : `Lance · v${data.version} · ${data.total.toLocaleString()} rows`}</span>
        {browse}
        {data.total > 0 && (
          <PageControls
            pageIndex={cursor.page}
            pageCount={Math.ceil(data.total / pageSize)}
            hasMore={data.offset + data.rows.length < data.total}
            loading={query.isFetching}
            showLoadingStatus
            onPrev={() => jump(cursor.page - 1)}
            onNext={() => jump(cursor.page + 1)}
            onJump={jump}
          />
        )}
      </div>
      <DataTable
        columns={data.columns}
        rows={data.rows}
        loading={false}
        pageIndex={data.offset / pageSize}
        pageSize={pageSize}
      />
    </div>
  )
}
